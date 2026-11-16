"""Local web console (stdlib http.server, bound to 127.0.0.1).

The server is generic so other tools can combine features on one page: ``serve`` takes the
page HTML and a map of URL prefix -> API object. An API object has

    state() -> dict                         merged into GET /api/state
    post(action, body) -> (status, dict)    POST <prefix><action>

The server adds the request guards, ``GET /api/dirs`` for the folder picker, and turns
``ValueError``/``OSError``/``KeyError`` from an API into a 400.

cbz-webp has an identical copy of the generic part; keep the two in step.
"""

from __future__ import annotations

import json
import os
import string
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


class API(Protocol):
    """``state`` and ``post`` are required. An API may also define

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
):
    """``allowed_origins``: exact extra origins (such as ``chrome-extension://<id>``) whose POSTs are
    accepted besides the page's own. Browsers set ``Origin`` themselves, so pages and other
    extensions can't claim one of these. ``pages``: more HTML pages by path (``{"/library": ...}``)."""
    all_pages = {"/": page, **(pages or {})}
    # Longest prefix first, so a more specific prefix ("/api/download/") wins over "/api/".
    routes = sorted(apis.items(), key=lambda kv: len(kv[0]), reverse=True)
    extra_origins = frozenset(allowed_origins)

    class Handler(BaseHTTPRequestHandler):
        server_version = "local-console"

        def log_message(self, fmt, *args):  # keep the terminal quiet
            pass

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

        def _json(self, obj, status: int = 200) -> None:
            self._send(status, json.dumps(obj).encode(), "application/json")

        def _host_ok(self) -> bool:
            port = self.server.server_address[1]
            return self.headers.get("Host", "") in (f"127.0.0.1:{port}", f"localhost:{port}")

        def _origin_ok(self) -> bool:
            origin = self.headers.get("Origin")
            if origin is None:
                return True
            if origin in extra_origins:
                return True
            return urlsplit(origin).scheme == "http" and urlsplit(origin).netloc == self.headers.get("Host")

        def do_GET(self):
            # Checking Host also stops DNS rebinding from reading the API.
            if not self._host_ok():
                return self._send(HTTPStatus.FORBIDDEN, b"bad host", "text/plain")
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
                    return self._send(status, body, headers.pop("Content-Type"), headers)
            self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")

        def do_POST(self):
            # This page can delete files: refuse anything a cross-site form could send.
            if not (self._host_ok() and self._origin_ok()):
                return self._json({"error": "forbidden"}, HTTPStatus.FORBIDDEN)
            if self.headers.get("Content-Type", "").split(";")[0].strip() != "application/json":
                return self._json({"error": "JSON only"}, HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
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
                        status, obj = api.post(path[len(prefix) :], body)
                    except (OSError, ValueError, KeyError, TypeError) as e:
                        return self._json({"error": str(e)}, HTTPStatus.BAD_REQUEST)
                    return self._json(obj, status)
            self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)

    return Handler


def serve(
    port: int,
    page: bytes,
    apis: dict[str, API],
    count_exts: Iterable[str] | None = None,
    allowed_origins: Iterable[str] = (),
    pages: dict[str, bytes] | None = None,
) -> ThreadingHTTPServer:
    handler = make_handler(page, apis, count_exts, allowed_origins, pages)
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    server.daemon_threads = True
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
