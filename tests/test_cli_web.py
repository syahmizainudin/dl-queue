"""Tests for `dl-queue get` and the web console's HTTP API."""

import contextlib
import http.client
import io
import json
import os
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock
from urllib.parse import quote

from helpers import FileServer

from dlqueue import __version__, cli
from dlqueue.basequeue import CANCELLED, DONE, FAILED
from dlqueue.download import DownloadQueue
from dlqueue.web import FileBody, current_client, current_user, serve, serve_downloads

KEY = FileServer.KEY


def run_cli(*argv, env=None, stdin=None):
    """Run the CLI in-process; return (exit code, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    stdin_patch = mock.patch("sys.stdin", stdin) if stdin is not None else contextlib.nullcontext()
    # patch.dict restores the real environment afterwards; start each run without a key set.
    with (
        mock.patch.dict(os.environ),
        stdin_patch,
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        os.environ.pop("DL_QUEUE_KEY", None)
        os.environ.update(env or {})
        try:
            code = cli.main(list(argv))
        except SystemExit as e:  # argparse errors and --version
            code = e.code
    return code, out.getvalue(), err.getvalue()


class TmpDir(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.files_server = FileServer()

    def tearDown(self):
        self.files_server.close()
        self.tmp.cleanup()

    def files(self):
        return sorted(p.name for p in self.dir.iterdir())

    def url(self, path):
        return self.files_server.base + path


class CliTests(TmpDir):
    def test_get_with_bearer_key_from_env(self):
        code, out, err = run_cli(
            "get", self.url("/secure/a.zip"), self.url("/files/x/b.bin"),
            "--to", str(self.dir), "--auth", "bearer", env={"DL_QUEUE_KEY": KEY},
        )  # fmt: skip
        self.assertEqual(code, 0, out + err)
        self.assertEqual(self.files(), ["a.zip", "b.bin"])
        self.assertIn("2 downloaded", out)
        self.assertNotIn(KEY, out + err)

    def test_custom_header_and_key_env_name(self):
        code, out, _ = run_cli(
            "get", self.url("/secure/a.zip"), "--to", str(self.dir),
            "--auth", "header", "--name", "X-API-Key", "--key-env", "MY_KEY", env={"MY_KEY": KEY},
        )  # fmt: skip
        self.assertEqual(code, 0, out)

    def test_query_param(self):
        code, out, _ = run_cli(
            "get", self.url("/secure/a.zip"), "--to", str(self.dir),
            "--auth", "query", "--name", "api_key", env={"DL_QUEUE_KEY": KEY},
        )  # fmt: skip
        self.assertEqual(code, 0, out)
        self.assertNotIn(KEY, out)

    def test_urls_from_input_file(self):
        lst = self.dir / "urls.txt"
        lst.write_text(f"# comment\n{self.url('/files/x/a.bin')}\n\n{self.url('/files/x/b.bin')}\n")
        code, out, _ = run_cli("get", "-i", str(lst), "--to", str(self.dir))
        self.assertEqual(code, 0, out)
        self.assertEqual(self.files(), ["a.bin", "b.bin", "urls.txt"])

    def test_urls_from_stdin(self):
        code, out, _ = run_cli(
            "get", "-i", "-", "--to", str(self.dir), stdin=io.StringIO(self.url("/files/x/a.bin"))
        )
        self.assertEqual(code, 0, out)
        self.assertEqual(self.files(), ["a.bin"])

    def test_failure_exit_code(self):
        code, out, _ = run_cli(
            "get",
            self.url("/secure/a.zip"),
            "--to",
            str(self.dir),
            "--auth",
            "bearer",
            env={"DL_QUEUE_KEY": "no"},
        )
        self.assertEqual(code, 1)
        self.assertIn("HTTP 401", out)
        self.assertEqual(self.files(), [])

    def test_missing_key_without_terminal(self):
        code, _, err = run_cli(
            "get", self.url("/secure/a.zip"), "--to", str(self.dir), "--auth", "bearer", stdin=io.StringIO("")
        )
        self.assertEqual(code, 2)
        self.assertIn("set DL_QUEUE_KEY", err)

    def test_key_prompt_on_terminal(self):
        tty = io.StringIO("")
        tty.isatty = lambda: True
        with mock.patch("getpass.getpass", return_value=KEY) as gp:
            code, out, _ = run_cli(
                "get", self.url("/secure/a.zip"), "--to", str(self.dir), "--auth", "bearer", stdin=tty
            )
        self.assertEqual(code, 0, out)
        gp.assert_called_once()

    def test_bad_arguments(self):
        self.assertEqual(run_cli("get", "--to", str(self.dir))[0], 2)  # no URLs
        self.assertEqual(run_cli("get", "ftp://x/y", "--to", str(self.dir))[0], 2)
        self.assertEqual(run_cli("get", self.url("/files/x/a"), "--to", str(self.dir / "nope"))[0], 2)
        self.assertEqual(run_cli("get", self.url("/files/x/a"))[0], 2)  # --to is required
        self.assertEqual(run_cli("get", self.url("/files/x/a"), "--to", str(self.dir), "-j", "0")[0], 2)
        self.assertEqual(run_cli("get", "x", "--to", str(self.dir), "--auth", "basic")[0], 2)
        code, _, err = run_cli(
            "get",
            self.url("/files/x/a"),
            "--to",
            str(self.dir),
            "--auth",
            "header",
            env={"DL_QUEUE_KEY": KEY},
        )
        self.assertEqual(code, 2)
        self.assertIn("invalid header name", err)
        self.assertEqual(self.files(), [])

    def test_version(self):
        code, out, _ = run_cli("--version")
        self.assertEqual(code, 0)
        self.assertIn(__version__, out)


class WebTests(TmpDir):
    def setUp(self):
        super().setUp()
        self.queue = DownloadQueue(workers=1, timeout=10).start()
        self.server = serve_downloads(0, self.queue)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.queue.shutdown()
        super().tearDown()

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        hdrs = {"Content-Type": "application/json"} if body is not None else {}
        hdrs.update(headers or {})
        data = json.dumps(body).encode() if isinstance(body, (dict, list)) else body
        conn.request(method, path, body=data, headers=hdrs)
        resp = conn.getresponse()
        raw = resp.read()
        conn.close()
        try:
            return resp.status, json.loads(raw)
        except ValueError:
            return resp.status, raw

    def post(self, path, body=None, **kw):
        return self.request("POST", path, {} if body is None else body, **kw)

    def state(self):
        return self.request("GET", "/api/state")[1]

    def test_page_and_state(self):
        status, page = self.request("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn(b"<title>Download Queue</title>", page)
        self.assertEqual(
            self.state(),
            {
                "downloads": [],
                "downloads_paused": False,
                "download_workers": 1,
                "download_rate": 0,
                "download_fair": False,
                "download_hours": None,
                "download_waiting": False,
                "download_lanes": [],
            },
        )

    def test_add_download_key_hidden(self):
        body = {
            "urls": [self.url("/secure/a.zip")],
            "dest": str(self.dir),
            "auth": {"mode": "header", "key": KEY, "name": "X-API-Key"},
        }
        self.assertEqual(self.post("/api/download/add", body), (200, {"added": 1, "duplicates": 0}))
        self.queue.wait_idle()
        st = self.state()
        self.assertNotIn(KEY, json.dumps(st))
        (dl,) = st["downloads"]
        self.assertEqual((dl["state"], dl["auth_mode"], dl["name"]), (DONE, "header", "a.zip"))
        self.assertEqual(self.files(), ["a.zip"])

    def test_lanes_in_state_and_add(self):
        from dlqueue.download import Lane

        self.queue.set_lanes([Lane("night", workers=2, rate=1024, hours=(60, 420))])
        (lane,) = self.state()["download_lanes"]
        self.assertEqual(
            {k: lane[k] for k in ("name", "workers", "rate", "hours", "running", "queued")},
            {"name": "night", "workers": 2, "rate": 1024, "hours": "01:00-07:00", "running": 0, "queued": 0},
        )
        body = {"urls": [self.url("/files/a/b.zip")], "dest": str(self.dir), "lane": "night"}
        self.assertEqual(self.post("/api/download/add", body)[0], 200)
        self.assertEqual(self.state()["downloads"][0]["lane"], "night")
        status, err = self.post("/api/download/add", {**body, "lane": "day"})
        self.assertEqual((status, err), (400, {"error": "there is no download lane 'day'"}))
        self.assertEqual(self.post("/api/download/add", {**body, "lane": 5})[0], 400)

    def test_lane_pause(self):
        from dlqueue.download import Lane

        self.queue.set_lanes([Lane("night")])
        status, out = self.post("/api/download/lane-pause", {"name": "night", "paused": True})
        self.assertEqual((status, out["lanes"][0]["paused"]), (200, True))
        self.assertTrue(self.state()["download_lanes"][0]["paused"])
        self.assertEqual(self.post("/api/download/lane-pause", {"name": "night", "paused": False})[0], 200)
        self.assertFalse(self.state()["download_lanes"][0]["paused"])
        status, err = self.post("/api/download/lane-pause", {"name": "day"})
        self.assertEqual((status, err), (400, {"error": "there is no download lane 'day'"}))
        self.assertEqual(self.post("/api/download/lane-pause", {"name": "night", "paused": "yes"})[0], 400)
        self.assertFalse(self.state()["download_fair"])

    def test_add_with_auth_ref(self):
        body = {"urls": [self.url("/files/r.zip")], "dest": str(self.dir), "auth_ref": "auto"}
        self.assertEqual(self.post("/api/download/add", body)[0], 200)
        self.assertEqual(self.state()["downloads"][0]["auth_ref"], "auto")
        self.assertEqual(self.post("/api/download/add", {**body, "auth_ref": 1})[0], 400)

    def test_add_validation(self):
        good = {"urls": ["http://127.0.0.1:1/x.zip"], "dest": str(self.dir)}
        cases = [
            {**good, "urls": []},
            {**good, "urls": "http://x/y"},
            {**good, "urls": ["ftp://x/y"]},
            {**good, "dest": ""},
            {**good, "dest": str(self.dir / "missing")},
            {**good, "auth": {"mode": "bearer", "key": ""}},
            {**good, "auth": {"mode": "header", "key": "k", "name": "a:b"}},
            {**good, "auth": {"mode": "nope", "key": "k"}},
            {**good, "auth": "bearer"},
            {**good, "after": [1]},
        ]
        for body in cases:
            with self.subTest(body=body):
                self.assertEqual(self.post("/api/download/add", body)[0], 400)
        self.assertEqual(self.post("/api/download/nope")[0], 404)
        self.assertEqual(self.post("/api/add", good)[0], 404)  # no convert API here
        self.assertEqual(self.state()["downloads"], [])

    def test_controls(self):
        self.post("/api/download/pause", {"paused": True})
        self.assertTrue(self.state()["downloads_paused"])
        body = {"urls": ["http://127.0.0.1:1/a.zip", "http://127.0.0.1:1/b.zip"], "dest": str(self.dir)}
        self.post("/api/download/add", body)
        ids = [d["id"] for d in self.state()["downloads"]]
        self.assertEqual(self.post("/api/download/cancel", {"id": ids[0]})[1], {"ok": True})
        self.assertEqual(self.post("/api/download/cancel-all")[1], {"cancelled": 1})
        self.assertEqual(self.post("/api/download/retry", {"id": ids[0]})[1], {"ok": True})
        self.post("/api/download/pause", {"paused": False})
        self.queue.wait_idle()
        states = {d["id"]: d["state"] for d in self.state()["downloads"]}
        self.assertEqual(states, {ids[0]: FAILED, ids[1]: CANCELLED})
        self.assertEqual(self.post("/api/download/clear")[1], {"cleared": 2})

    def test_dirs_counts_all_files(self):
        (self.dir / "sub").mkdir()
        (self.dir / ".hidden").mkdir()
        (self.dir / "a.txt").write_text("x")
        (self.dir / "b.zip").write_text("x")
        status, r = self.request("GET", "/api/dirs?path=" + quote(str(self.dir)))
        self.assertEqual(status, 200, r)
        self.assertEqual((r["dirs"], r["files"]), (["sub"], 2))
        self.assertEqual(self.request("GET", "/api/dirs?path=" + quote(str(self.dir / "a.txt")))[0], 400)

    # Downloads write files, so these guards matter.
    def test_guards(self):
        body = {"urls": ["http://127.0.0.1:1/a.zip"], "dest": str(self.dir)}
        self.assertEqual(
            self.post("/api/download/add", body, headers={"Origin": "http://evil.example"})[0], 403
        )
        status, _ = self.request(
            "POST", "/api/download/add", b"urls=x", {"Content-Type": "application/x-www-form-urlencoded"}
        )
        self.assertEqual(status, 415)
        self.assertEqual(self.request("GET", "/api/state", headers={"Host": "evil.example"})[0], 403)
        self.assertEqual(self.state()["downloads"], [])

    def test_allowed_origins(self):
        from dlqueue.web import UI_HTML, DownloadAPI, serve

        server = serve(
            0,
            UI_HTML,
            {"/api/download/": DownloadAPI(self.queue)},
            allowed_origins={"chrome-extension://abc"},
        )
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.port, port = server.server_address[1], self.port
        try:
            body = {"paused": True}
            ok = self.post("/api/download/pause", body, headers={"Origin": "chrome-extension://abc"})
            self.assertEqual(ok[0], 200)
            bad = self.post("/api/download/pause", body, headers={"Origin": "chrome-extension://xyz"})
            self.assertEqual(bad[0], 403)
        finally:
            self.port = port


class AuthServerTests(TmpDir):
    """With ``auth``, the server asks it who each request is from and refuses the signed-out."""

    class FakeAuth:
        login_path = "/login"

        def public(self, method, path):
            return (method, path) in {("GET", "/login"), ("POST", "/api/me/login")}

        def user(self, headers):
            return {"t1": "ann", "t2": "bo"}.get(headers.get("X-Token", ""))

        def allows(self, user, method, path):
            return not (user == "bo" and path == "/api/dirs")  # bo may not browse folders

    class MeAPI:
        def __init__(self):
            self.seen = []

        def state(self):
            return {"me": current_user()}

        def post(self, action, body):
            self.seen.append(current_user())
            if action == "login":
                return 200, {"ok": True}, {"Set-Cookie": "s=1; HttpOnly"}
            return 200, {"user": current_user()}

        def get(self, action, query):
            return 200, {"Content-Type": "application/json"}, json.dumps({"user": current_user()}).encode()

    def setUp(self):
        super().setUp()
        self.me = self.MeAPI()
        self.server = serve(
            0, b"<title>Main</title>", {"/api/me/": self.me}, pages={"/login": b"login"}, auth=self.FakeAuth()
        )
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        super().tearDown()

    def req(self, method, path, token=None, body=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        headers = {"X-Token": token} if token else {}
        if body is not None:
            headers["Content-Type"] = "application/json"
        conn.request(
            method, path, body=json.dumps(body).encode() if body is not None else None, headers=headers
        )
        resp = conn.getresponse()
        data, h = resp.read(), dict(resp.getheaders())
        conn.close()
        return resp.status, h, data

    def test_signed_out_requests_are_refused(self):
        status, h, _ = self.req("GET", "/")
        self.assertEqual((status, h["Location"]), (303, "/login"))
        for method, path, body in (
            ("GET", "/api/state", None),
            ("GET", "/api/dirs", None),
            ("GET", "/api/me/x", None),
            ("POST", "/api/me/x", {}),
        ):
            with self.subTest(path=path):
                self.assertEqual(self.req(method, path, body=body)[0], 401)
        self.assertEqual(self.me.seen, [])
        self.assertEqual(self.req("GET", "/login")[2], b"login")  # public

    def test_public_post_can_set_headers(self):
        status, h, data = self.req("POST", "/api/me/login", body={})
        self.assertEqual((status, h["Set-Cookie"], json.loads(data)), (200, "s=1; HttpOnly", {"ok": True}))
        self.assertEqual(self.me.seen, [None])  # public, signed out

    def test_each_request_sees_its_own_user(self):
        self.assertEqual(self.req("GET", "/", token="t1")[0], 200)
        self.assertEqual(json.loads(self.req("GET", "/api/state", token="t1")[2]), {"me": "ann"})
        self.assertEqual(json.loads(self.req("GET", "/api/me/x", token="t2")[2]), {"user": "bo"})
        self.assertEqual(json.loads(self.req("POST", "/api/me/x", token="t2", body={})[2]), {"user": "bo"})
        self.assertIsNone(current_user())  # nothing leaks outside a request
        self.assertEqual(self.req("GET", "/api/state", token="nope")[0], 401)

    def test_allows_can_refuse_a_signed_in_user(self):
        self.assertEqual(self.req("GET", "/api/dirs", token="t1")[0], 200)
        status, _, data = self.req("GET", "/api/dirs", token="t2")
        self.assertEqual((status, json.loads(data)["error"]), (403, "you don't have permission for this"))
        self.assertEqual(self.req("GET", "/api/state", token="t2")[0], 200)

    def test_without_auth_there_is_no_user(self):
        self.assertIsNone(current_user())


class ReachTests(TmpDir):
    """Allowed host names, HTTPS, a trusted reverse proxy, the client address, and file bodies."""

    class API:
        def __init__(self, path):
            self.path = path

        def state(self):
            return {"client": current_client()}

        def post(self, action, body):
            return 200, {"client": current_client()}

        def get(self, action, query):
            if action == "file":
                return 200, {"Content-Type": "application/octet-stream"}, FileBody(self.path)
            return 404, {"Content-Type": "application/json"}, b"{}"

    def start(self, **kw):
        self.blob = self.dir / "blob.bin"
        self.data = os.urandom(700_000)
        self.blob.write_bytes(self.data)
        server = serve(0, b"<title>p</title>", {"/api/x/": self.API(self.blob)}, **kw)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server.server_address[1]

    def req(self, port, method, path, headers=None, body=None, context=None):
        if context is not None:
            conn = http.client.HTTPSConnection("127.0.0.1", port, timeout=10, context=context)
        else:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        conn.putrequest(method, path, skip_host=True)
        h = {"Host": f"127.0.0.1:{port}", **(headers or {})}
        data = json.dumps(body).encode() if body is not None else b""
        if body is not None:
            h["Content-Type"] = "application/json"
            h["Content-Length"] = str(len(data))
        for k, v in h.items():
            conn.putheader(k, v)
        conn.endheaders(data or None)
        resp = conn.getresponse()
        raw, rh = resp.read(), dict(resp.getheaders())
        conn.close()
        return resp.status, rh, raw

    def test_allowed_host_names(self):
        port = self.start(allowed_hosts=["harbor.lan", "192.168.1.5"])
        for host in (
            "harbor.lan",
            f"harbor.lan:{port}",
            "HARBOR.LAN:443",
            f"192.168.1.5:{port}",
            f"localhost:{port}",
        ):
            self.assertEqual(self.req(port, "GET", "/", {"Host": host})[0], 200, host)
        for host in ("evil.example", f"harbor.lan.evil.example:{port}", "", f"127.0.0.1:{port + 1}"):
            self.assertEqual(self.req(port, "GET", "/", {"Host": host})[0], 403, host)
        ok = {"Host": "harbor.lan:8765", "Origin": "http://harbor.lan:8765"}
        self.assertEqual(self.req(port, "POST", "/api/x/a", ok, {})[0], 200)
        bad = {"Host": "harbor.lan:8765", "Origin": "http://evil.example"}
        self.assertEqual(self.req(port, "POST", "/api/x/a", bad, {})[0], 403)
        # without allowed_hosts nothing changes
        port2 = self.start()
        self.assertEqual(self.req(port2, "GET", "/", {"Host": "harbor.lan"})[0], 403)

    def test_https(self):
        if not shutil.which("openssl"):
            self.skipTest("needs openssl to make a certificate")
        cert, key = self.dir / "c.pem", self.dir / "k.pem"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=127.0.0.1",
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=True,
            capture_output=True,
        )
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert, key)
        port = self.start(ssl_context=ctx)
        client = ssl.create_default_context(cafile=str(cert))
        client.check_hostname = False
        self.assertEqual(self.req(port, "GET", "/", context=client)[0], 200)
        same = {"Origin": f"https://127.0.0.1:{port}"}
        self.assertEqual(self.req(port, "POST", "/api/x/a", same, {}, context=client)[0], 200)
        plain = {"Origin": f"http://127.0.0.1:{port}"}  # wrong scheme: not this page
        self.assertEqual(self.req(port, "POST", "/api/x/a", plain, {}, context=client)[0], 403)
        # a plain-HTTP client gets no page; the server carries on
        with self.assertRaises((http.client.HTTPException, ConnectionError, OSError)):
            self.req(port, "GET", "/")
        self.assertEqual(self.req(port, "GET", "/", context=client)[0], 200)

    def test_trusted_proxy(self):
        port = self.start(allowed_hosts=["books.example"], trust_proxy=True)
        fwd = {
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "books.example",
            "X-Forwarded-For": "203.0.113.9, 10.0.0.1",
        }
        status, _, raw = self.req(port, "POST", "/api/x/a", {**fwd, "Origin": "https://books.example"}, {})
        self.assertEqual((status, json.loads(raw)), (200, {"client": "203.0.113.9"}))
        self.assertEqual(
            self.req(port, "POST", "/api/x/a", {**fwd, "Origin": "http://books.example"}, {})[0], 403
        )
        self.assertEqual(self.req(port, "GET", "/", {**fwd, "X-Forwarded-Host": "evil.example"})[0], 403)
        junk = {**fwd, "X-Forwarded-For": "not an address"}
        self.assertEqual(
            json.loads(
                self.req(port, "POST", "/api/x/a", {**junk, "Origin": "https://books.example"}, {})[2]
            ),
            {"client": "127.0.0.1"},
        )
        # not trusted: the forwarded headers mean nothing
        port2 = self.start(allowed_hosts=["books.example"])
        self.assertEqual(
            self.req(port2, "POST", "/api/x/a", {**fwd, "Origin": "https://books.example"}, {})[0], 403
        )
        status, _, raw = self.req(port2, "POST", "/api/x/a", {"X-Forwarded-For": "203.0.113.9"}, {})
        self.assertEqual(json.loads(raw), {"client": "127.0.0.1"})

    def test_file_body_and_ranges(self):
        port = self.start()
        status, h, raw = self.req(port, "GET", "/api/x/file")
        self.assertEqual(
            (status, raw == self.data, h["Content-Length"], h["Accept-Ranges"]),
            (200, True, str(len(self.data)), "bytes"),
        )
        status, h, raw = self.req(port, "GET", "/api/x/file", {"Range": "bytes=10-19"})
        self.assertEqual(
            (status, raw, h["Content-Range"]), (206, self.data[10:20], f"bytes 10-19/{len(self.data)}")
        )
        status, _, raw = self.req(port, "GET", "/api/x/file", {"Range": "bytes=-5"})
        self.assertEqual((status, raw), (206, self.data[-5:]))
        status, _, raw = self.req(port, "GET", "/api/x/file", {"Range": "bytes=699990-"})
        self.assertEqual((status, raw), (206, self.data[699990:]))
        status, h, _ = self.req(port, "GET", "/api/x/file", {"Range": f"bytes={len(self.data)}-"})
        self.assertEqual((status, h["Content-Range"]), (416, f"bytes */{len(self.data)}"))
        self.assertEqual(
            self.req(port, "GET", "/api/x/file", {"Range": "bytes=0-1,4-5"})[0], 200
        )  # one range only
        for junk in ("bytes=abc-", "bytes=-", "bytes=1-x", "items=0-1"):  # malformed: ignored
            self.assertEqual(self.req(port, "GET", "/api/x/file", {"Range": junk})[0], 200, junk)


if __name__ == "__main__":
    unittest.main()
