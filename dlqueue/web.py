"""Local web console (stdlib http.server, bound to 127.0.0.1).

The server is generic so other tools can combine features on one page: ``serve`` takes the
page HTML and a map of URL prefix -> API object. An API object has

    state() -> dict                         merged into GET /api/state
    post(action, body) -> (status, dict)    POST <prefix><action>
                       or (status, dict, headers)

The server adds the request guards, ``GET /api/dirs`` for the folder picker, and turns
``ValueError``/``OSError``/``KeyError`` from an API into a 400. With ``auth``, every request
except the ones ``auth`` makes public needs a signed-in user, and ``current_user()`` names it.

By default it listens on 127.0.0.1 only. A host can bind another address with ``host``, name the
host names it may be reached by (``allowed_hosts``), serve HTTPS (``ssl_context``), or sit behind
a reverse proxy on the same machine (``trust_proxy``); deciding when that is safe is the host's job.

cbz-webp has an identical copy of the generic part; keep the two in step.
"""

from __future__ import annotations

import ipaddress
import json
import os
import socket
import ssl
import string
import sys
import threading
from collections.abc import Iterable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Protocol
from urllib.parse import parse_qs, urlsplit

from .download import Auth, DownloadQueue
from .paths import normalize_path

UI_HTML = (Path(__file__).parent / "ui.html").read_bytes()


# ---------------------------------------------------------------- generic server part
# Everything from here to "end of generic server part" is identical in dl-queue's web.py.


_request = threading.local()  # one thread per request (ThreadingHTTPServer)


def current_user():
    """The signed-in user of the request being handled, as ``auth.user`` returned it.
    None without ``auth``, or for a public request from someone signed out."""
    return getattr(_request, "user", None)


def current_client() -> str:
    """The address of the client that sent the request being handled: the connection's, or with
    ``trust_proxy`` and a connection from this machine, the first ``X-Forwarded-For`` address."""
    return getattr(_request, "client", "")


LOOPBACK = frozenset({"127.0.0.1", "::1", "::ffff:127.0.0.1"})
CHUNK = 256 * 1024


class FileBody:
    """A file an API's ``get`` returns as the body, instead of bytes: the server streams it in
    chunks and answers a single ``Range: bytes=`` request with 206. The API finds the path itself
    (by id); it never comes from the request."""

    def __init__(self, path, filename: str | None = None):
        self.path = Path(path)
        self.size = self.path.stat().st_size
        self.filename = filename


def _byte_range(header: str | None, size: int) -> tuple[int, int] | None:
    """(first, last) for a single ``bytes=`` range, None for the whole file. ValueError if it
    can't be satisfied."""
    if not header or not header.startswith("bytes=") or "," in header:
        return None
    first, _, last = header[6:].strip().partition("-")
    if not first:  # the last N bytes
        n = int(last)
        if n <= 0:
            raise ValueError("bad range")
        return max(size - n, 0), size - 1
    a, b = int(first), (int(last) if last else size - 1)
    if a >= size or b < a:
        raise ValueError("bad range")
    return a, min(b, size - 1)


class SignIn(Protocol):
    """Sign-in for ``serve(auth=...)``. The host owns accounts and sessions; the server only asks.

        public(method, path) -> bool    requests allowed signed out (the sign-in page and action)
        user(headers) -> object | None  who sent the request (from a session cookie or a token)
        login_path                      where a signed-out browser asking for a page is sent

    Signed out, a page request gets 303 to ``login_path`` and anything else 401. Optionally,

        allows(user, method, path) -> bool   False answers 403 (for permissions the server
                                             itself serves, such as ``/api/dirs``)
    """

    login_path: str

    def public(self, method: str, path: str) -> bool: ...

    def user(self, headers) -> object | None: ...


class API(Protocol):
    """``state`` and ``post`` are required. ``post`` may return a third item, extra response
    headers (such as ``Set-Cookie``). An API may also define

        get(action, query) -> (status, headers, body)

    for ``GET <prefix><action>?...`` (``query`` holds the last value of each parameter;
    ``headers`` must include ``Content-Type``). Without ``Cache-Control`` the response is
    ``no-store``.
    """

    def state(self) -> dict: ...

    def post(self, action: str, body: dict) -> tuple[int, dict]: ...


def list_dirs(raw: str | None, count_exts: Iterable[str] | None = None) -> dict:
    """Folders inside ``raw`` (default: home) for the page's folder picker. Read-only.

    ``files`` counts the files directly inside with an extension in ``count_exts`` (all files
    when None).
    """
    p = normalize_path(raw) if raw and raw.strip() else Path.home().resolve()
    if not p.exists():
        raise ValueError(f"not found: {p}")
    if not p.is_dir():
        raise ValueError(f"not a folder: {p}")
    exts = None if count_exts is None else {e.lower() for e in count_exts}
    dirs, files = [], 0
    with os.scandir(p) as it:
        for e in it:
            if e.name.startswith("."):
                continue
            try:
                if e.is_dir():
                    dirs.append(e.name)
                elif exts is None or Path(e.name).suffix.lower() in exts:
                    files += 1
            except OSError:
                continue
    shortcuts = [{"label": "Home", "path": str(Path.home())}]
    for letter in string.ascii_lowercase:
        drive = Path("/mnt") / letter
        if drive.is_dir():
            shortcuts.append({"label": f"{letter.upper()}:", "path": str(drive)})
    return {
        "path": str(p),
        "parent": str(p.parent) if p.parent != p else None,
        "dirs": sorted(dirs, key=str.lower),
        "files": files,
        "shortcuts": shortcuts,
    }


def make_handler(
    page: bytes,
    apis: dict[str, API],
    count_exts: Iterable[str] | None = None,
    allowed_origins: Iterable[str] = (),
    pages: dict[str, bytes] | None = None,
    auth: SignIn | None = None,
    allowed_hosts: Iterable[str] = (),
    https: bool = False,
    trust_proxy: bool = False,
):
    """``allowed_origins``: exact extra origins (such as ``chrome-extension://<id>``) whose POSTs are
    accepted besides the page's own. Browsers set ``Origin`` themselves, so pages and other
    extensions can't claim one of these. ``pages``: more HTML pages by path (``{"/library": ...}``).

    ``allowed_hosts``: host names (no port) the server may be reached by, besides 127.0.0.1 and
    localhost on its own port. ``https``: the socket is TLS. ``trust_proxy``: a request from this
    machine may say, in ``X-Forwarded-Proto``/``-Host``/``-For``, how the client reached the proxy."""
    all_pages = {"/": page, **(pages or {})}
    # Longest prefix first, so a more specific prefix ("/api/download/") wins over "/api/".
    routes = sorted(apis.items(), key=lambda kv: len(kv[0]), reverse=True)
    extra_origins = frozenset(allowed_origins)
    names = frozenset(h.strip().lower().rstrip(".") for h in allowed_hosts if h.strip())

    class Handler(BaseHTTPRequestHandler):
        server_version = "local-console"
        timeout = 120  # a client that stops sending doesn't hold a thread for ever

        def log_message(self, fmt, *args):  # keep the terminal quiet
            pass

        def setup(self):
            if isinstance(self.request, ssl.SSLSocket):  # the handshake runs here, not in accept()
                self.request.settimeout(15)
                self.request.do_handshake()
            super().setup()

        def _proxied(self) -> bool:
            return trust_proxy and self.client_address[0] in LOOPBACK

        def _scheme(self) -> str:
            if self._proxied():
                proto = self.headers.get("X-Forwarded-Proto", "").split(",")[0].strip().lower()
                if proto in ("http", "https"):
                    return proto
            return "https" if https else "http"

        def _host(self) -> str:
            """The host the client asked for: ``Host``, or a trusted proxy's ``X-Forwarded-Host``."""
            if self._proxied() and self.headers.get("X-Forwarded-Host"):
                return self.headers["X-Forwarded-Host"].split(",")[0].strip()
            return self.headers.get("Host", "")

        def _client(self) -> str:
            if self._proxied() and self.headers.get("X-Forwarded-For"):
                first = self.headers["X-Forwarded-For"].split(",")[0].strip()
                try:
                    return str(ipaddress.ip_address(first))
                except ValueError:
                    pass
            return self.client_address[0]

        def _send(self, status: int, body: bytes, ctype: str, headers: dict | None = None) -> None:
            headers = dict(headers or {})
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", headers.pop("Cache-Control", "no-store"))
            self.send_header("X-Content-Type-Options", "nosniff")
            # Pages may frame each other (a tab showing another tool's page); other sites may not.
            self.send_header("X-Frame-Options", "SAMEORIGIN")
            for k, v in headers.items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _json(self, obj, status: int = 200, headers: dict | None = None) -> None:
            self._send(status, json.dumps(obj).encode(), "application/json", headers)

        def _signed_in(self, method: str, path: str) -> bool:
            """Record the request's user. False after answering a signed-out request."""
            _request.user = None
            if auth is None:
                return True
            _request.user = auth.user(self.headers)
            if _request.user is not None:
                allows = getattr(auth, "allows", None)
                if allows is not None and not allows(_request.user, method, path):
                    self._json({"error": "you don't have permission for this"}, HTTPStatus.FORBIDDEN)
                    return False
                return True
            if auth.public(method, path):
                return True
            if method == "GET" and path in all_pages:
                self._send(HTTPStatus.SEE_OTHER, b"", "text/plain", {"Location": auth.login_path})
            else:
                self._json({"error": "sign in first"}, HTTPStatus.UNAUTHORIZED)
            return False

        def _host_ok(self) -> bool:
            host, port = self._host(), self.server.server_address[1]
            if host in (f"127.0.0.1:{port}", f"localhost:{port}"):
                return True
            try:
                name = urlsplit("//" + host).hostname or ""
            except ValueError:
                return False
            return name.rstrip(".") in names

        def _origin_ok(self) -> bool:
            origin = self.headers.get("Origin")
            if origin is None:
                return True
            if origin in extra_origins:
                return True
            return urlsplit(origin).scheme == self._scheme() and urlsplit(origin).netloc == self._host()

        def _send_file(self, status: int, body: FileBody, ctype: str, headers: dict) -> None:
            """Stream a file, or the one byte range asked for (206)."""
            try:
                span = _byte_range(self.headers.get("Range"), body.size)
            except ValueError:
                return self._send(
                    HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
                    b"",
                    "text/plain",
                    {"Content-Range": f"bytes */{body.size}"},
                )
            first, last = span or (0, body.size - 1)
            length = max(last - first + 1, 0)
            self.send_response(HTTPStatus.PARTIAL_CONTENT if span else status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(length))
            self.send_header("Accept-Ranges", "bytes")
            if span:
                self.send_header("Content-Range", f"bytes {first}-{last}/{body.size}")
            self.send_header("Cache-Control", headers.pop("Cache-Control", "no-store"))
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "SAMEORIGIN")
            for k, v in headers.items():
                self.send_header(k, v)
            self.end_headers()
            with open(body.path, "rb") as f:
                f.seek(first)
                left = length
                while left > 0:
                    chunk = f.read(min(CHUNK, left))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    left -= len(chunk)

        def do_GET(self):
            # Checking Host also stops DNS rebinding from reading the API.
            if not self._host_ok():
                return self._send(HTTPStatus.FORBIDDEN, b"bad host", "text/plain")
            _request.client = self._client()
            try:
                if self._signed_in("GET", urlsplit(self.path).path):
                    self._get()
            finally:
                _request.user = None
                _request.client = ""

        def _get(self):
            url = urlsplit(self.path)
            if url.path in all_pages:
                return self._send(200, all_pages[url.path], "text/html; charset=utf-8")
            if url.path == "/api/state":
                state: dict = {}
                for _, api in routes:
                    state.update(api.state())
                return self._json(state)
            if url.path == "/api/dirs":
                try:
                    raw = parse_qs(url.query).get("path", [""])[0]
                    return self._json(list_dirs(raw, count_exts))
                except (OSError, ValueError) as e:
                    return self._json({"error": str(e)}, HTTPStatus.BAD_REQUEST)
            for prefix, api in routes:
                action = url.path[len(prefix) :]
                if url.path.startswith(prefix) and "/" not in action and hasattr(api, "get"):
                    query = {k: v[-1] for k, v in parse_qs(url.query).items()}
                    try:
                        status, headers, body = api.get(action, query)
                    except (OSError, ValueError, KeyError, TypeError) as e:
                        return self._json({"error": str(e)}, HTTPStatus.BAD_REQUEST)
                    headers = dict(headers)
                    if isinstance(body, FileBody):
                        return self._send_file(status, body, headers.pop("Content-Type"), headers)
                    return self._send(status, body, headers.pop("Content-Type"), headers)
            self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")

        def do_POST(self):
            # This page can delete files: refuse anything a cross-site form could send.
            if not (self._host_ok() and self._origin_ok()):
                return self._json({"error": "forbidden"}, HTTPStatus.FORBIDDEN)
            if self.headers.get("Content-Type", "").split(";")[0].strip() != "application/json":
                return self._json({"error": "JSON only"}, HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
            _request.client = self._client()
            try:
                if self._signed_in("POST", urlsplit(self.path).path):
                    self._post()
            finally:
                _request.user = None
                _request.client = ""

        def _post(self):
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length) or b"{}")
                if not isinstance(body, dict):
                    raise ValueError("body must be a JSON object")
            except ValueError:
                return self._json({"error": "bad JSON"}, HTTPStatus.BAD_REQUEST)

            path = urlsplit(self.path).path
            for prefix, api in routes:
                if path.startswith(prefix) and "/" not in path[len(prefix) :]:
                    try:
                        status, obj, *extra = api.post(path[len(prefix) :], body)
                    except (OSError, ValueError, KeyError, TypeError) as e:
                        return self._json({"error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return self._json(obj, status, extra[0] if extra else None)
            self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)

    return Handler


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        # A client that isn't speaking TLS, or hangs up or stalls, isn't worth a traceback.
        if isinstance(sys.exc_info()[1], (ssl.SSLError, ConnectionError, TimeoutError)):
            return
        super().handle_error(request, client_address)


class _Server6(_Server):
    address_family = socket.AF_INET6


def serve(
    port: int,
    page: bytes,
    apis: dict[str, API],
    count_exts: Iterable[str] | None = None,
    allowed_origins: Iterable[str] = (),
    pages: dict[str, bytes] | None = None,
    auth: SignIn | None = None,
    *,
    host: str = "127.0.0.1",
    allowed_hosts: Iterable[str] = (),
    ssl_context: ssl.SSLContext | None = None,
    trust_proxy: bool = False,
) -> ThreadingHTTPServer:
    """The console server on ``host:port`` (127.0.0.1 unless the host asks otherwise)."""
    handler = make_handler(
        page,
        apis,
        count_exts,
        allowed_origins,
        pages,
        auth,
        allowed_hosts=allowed_hosts,
        https=ssl_context is not None,
        trust_proxy=trust_proxy,
    )
    server = (_Server6 if ":" in host else _Server)((host, port), handler)
    if ssl_context is not None:
        server.socket = ssl_context.wrap_socket(
            server.socket, server_side=True, do_handshake_on_connect=False
        )
    return server


# ---------------------------------------------------------------- end of generic server part


def serve_downloads(port: int, queue: DownloadQueue) -> ThreadingHTTPServer:
    """This package's console: the download page."""
    return serve(port, UI_HTML, {"/api/download/": DownloadAPI(queue)})


# ---------------------------------------------------------------- download API


class DownloadAPI:
    """The download queue's endpoints, mounted at ``/api/download/``."""

    def __init__(self, queue: DownloadQueue):
        self.queue = queue

    def state(self) -> dict:
        q = self.queue
        return {
            "downloads": q.snapshot(),
            "downloads_paused": q.paused,
            "download_workers": q.workers,
        }

    def post(self, action: str, body: dict) -> tuple[int, dict]:
        q = self.queue
        if action == "add":
            urls = body.get("urls", [])
            if not isinstance(urls, list):
                raise ValueError("urls must be a list")
            a = body.get("auth") or {}
            if not isinstance(a, dict):
                raise ValueError("auth must be an object")
            after = body.get("after") or {}
            if not isinstance(after, dict):
                raise ValueError("after must be an object")
            auth = Auth(str(a.get("mode", "none")), str(a.get("key", "")), str(a.get("name", "")))
            added, dupes = q.add([str(u) for u in urls], str(body.get("dest", "")), auth, after=after)
            return 200, {"added": len(added), "duplicates": dupes}
        if action == "cancel":
            return 200, {"ok": q.cancel(int(body["id"]))}
        if action == "cancel-all":
            return 200, {"cancelled": q.cancel_all()}
        if action == "retry":
            return 200, {"ok": q.retry(int(body["id"]))}
        if action == "clear":
            return 200, {"cleared": q.clear_finished()}
        if action == "pause":
            q.set_paused(bool(body.get("paused", True)))
            return 200, {"paused": q.paused}
        return 404, {"error": "not found"}
