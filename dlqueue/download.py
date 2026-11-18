"""Download queue: fetch direct file URLs into a folder, with an optional API key.

After a file is saved, an optional ``on_saved(job, path)`` hook runs. Tools built on this
(harbor, for example) use it to pass archives on to another queue; ``job.after`` carries the
options for that step and the hook can set ``job.linked_id`` and ``job.message``.

Rules (see AGENTS.md):
  * the API key never appears in job state, logs or messages; in query mode the stored URL has
    no key and it is added only when the request is made;
  * auth headers are sent with ``add_unredirected_header`` so a redirect to another host (a CDN)
    never receives the key;
  * data is streamed to ``<name>.part`` in the destination folder and renamed when complete;
  * temporary failures (timeouts, dropped connections, 5xx, 408/429, short reads) are retried
    with backoff; the ``.part`` is kept between attempts and continued with an HTTP Range request
    when the server supports it and the file hasn't changed (``If-Range``). The final failure or
    a cancel removes the ``.part``.
"""

from __future__ import annotations

import http.client
import os
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit
from email.message import Message
from urllib.request import Request, urlopen

from . import __version__
from .basequeue import ACTIVE, CANCELLED, DONE, FAILED, QUEUED, BaseQueue
from .paths import PART_SUFFIX, normalize_path

AUTH_MODES = ("none", "bearer", "header", "query")
TOKEN_RE = re.compile(r"^[A-Za-z0-9!#$%&'*+.^_`|~-]+$")  # RFC 9110 header-name token
CHUNK = 256 * 1024
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}
MAX_DELAY = 60.0  # cap for backoff and Retry-After, in seconds
USER_AGENT = f"dl-queue/{__version__}"


class DownloadCancelled(Exception):
    pass


class DownloadError(Exception):
    pass


class _Transient(Exception):
    """A failure worth retrying."""

    def __init__(self, reason: str, retry_after: float | None = None):
        super().__init__(reason)
        self.reason = reason
        self.retry_after = retry_after


def _retry_after(headers) -> float | None:
    try:
        return max(0.0, float(headers.get("Retry-After", "")))
    except (TypeError, ValueError):
        return None  # absent, or an HTTP date: use the normal backoff


@dataclass
class Auth:
    mode: str = "none"  # none | bearer | header | query
    key: str = field(default="", repr=False)
    name: str = ""  # header name (mode=header) or query parameter (mode=query)

    def validate(self) -> Auth:
        if self.mode not in AUTH_MODES:
            raise ValueError(f"auth mode must be one of {', '.join(AUTH_MODES)}")
        if self.mode == "none":
            return Auth()
        if not self.key:
            raise ValueError("API key is empty")
        if any(c in self.key for c in "\r\n\0"):
            raise ValueError("API key contains a line break")
        if self.mode in ("header", "query") and not TOKEN_RE.match(self.name or ""):
            raise ValueError(f"invalid {'header' if self.mode == 'header' else 'parameter'} name")
        return self

    def apply(self, url: str) -> tuple[str, dict[str, str]]:
        """Return the URL to request and the headers that carry the key."""
        if self.mode == "bearer":
            return url, {"Authorization": f"Bearer {self.key}"}
        if self.mode == "header":
            return url, {self.name: self.key}
        if self.mode == "query":
            parts = urlsplit(url)
            query = [*parse_qsl(parts.query, keep_blank_values=True), (self.name, self.key)]
            return urlunsplit(parts._replace(query=urlencode(query))), {}
        return url, {}


def check_url(url: str) -> str:
    url = url.strip()
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError(f"not an http(s) URL: {url}")
    return url


def safe_name(raw: str) -> str:
    """Reduce a server- or URL-supplied name to a bare, Windows-safe file name ('' if nothing is left)."""
    name = unquote(raw).replace("\\", "/").split("/")[-1]
    name = re.sub(r'[\x00-\x1f<>:"|?*]', "_", name).strip().lstrip(".").strip()
    return name[:200]


def disposition_name(header: str | None) -> str:
    """File name from a Content-Disposition header; RFC 6266 says ``filename*`` wins over ``filename``."""
    if not header:
        return ""
    m = re.search(r"filename\*\s*=\s*([\w!#$%&+^`{}~-]*)'[^']*'([^;\s]+)", header, re.I)
    if m:
        try:
            return unquote(m.group(2), encoding=m.group(1) or "utf-8", errors="strict")
        except (LookupError, UnicodeDecodeError):
            pass
    msg = Message()
    msg["Content-Disposition"] = header
    return msg.get_filename() or ""


@dataclass
class Download:
    id: int
    url: str  # never contains the key
    dest_dir: str
    auth: Auth = field(repr=False)
    after: dict = field(default_factory=dict)  # options for the on_saved hook; never secret
    state: str = QUEUED
    done: int = 0  # bytes received
    total: int = 0  # Content-Length, 0 when the server did not send one
    message: str = ""
    filename: str | None = None
    output: str | None = None
    linked_id: int | None = None  # set by the on_saved hook, e.g. a convert job id
    attempts: int = 0
    added_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    cancel: threading.Event = field(default_factory=threading.Event, repr=False)

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k not in ("auth", "cancel")}
        d["auth_mode"] = self.auth.mode  # the key itself is never exposed
        d["name"] = self.filename or safe_name(urlsplit(self.url).path) or self.url
        return d

    def fresh(self) -> Download:
        return Download(
            self.id,
            self.url,
            self.dest_dir,
            self.auth,
            dict(self.after),
        )


class DownloadQueue(BaseQueue):
    thread_prefix = "download"

    def __init__(
        self,
        workers: int = 2,
        *,
        on_saved: Callable[[Download, Path], None] | None = None,
        check_after: Callable[[dict], dict] | None = None,
        timeout: float = 60,
        retries: int = 3,
        backoff: float = 1.0,
        on_change: Callable | None = None,
    ):
        """``check_after`` validates/normalizes ``after`` when jobs are added (raise ValueError).

        ``retries`` extra attempts follow a temporary failure, waiting ``backoff`` seconds, then
        twice that, and so on (or the server's ``Retry-After``), capped at a minute.
        """
        super().__init__(workers, on_change)
        if retries < 0 or backoff < 0:
            raise ValueError("retries and backoff must not be negative")
        self.on_saved = on_saved
        self.check_after = check_after
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self._reserved: set[Path] = set()

    # ------------------------------------------------------------ commands
    def add(
        self,
        urls: list[str],
        dest_dir: str | Path,
        auth: Auth | None = None,
        *,
        after: dict | None = None,
    ) -> tuple[list[Download], int]:
        """Queue each URL not already queued/running for the same folder. Returns (added, dupes)."""
        urls = [check_url(u) for u in urls if u.strip()]
        if not urls:
            raise ValueError("give at least one URL")
        if not str(dest_dir).strip():
            raise ValueError("choose a folder to save to")
        dest = normalize_path(str(dest_dir))
        if not dest.is_dir():
            raise ValueError(f"save-to folder does not exist: {dest}")
        after = dict(after or {})
        if self.check_after:
            after = self.check_after(after)
        auth = (auth or Auth()).validate()
        added, dupes = [], 0
        with self._cond:
            active = {self._dedup_key(j) for j in self._jobs.values() if j.state in ACTIVE}
            for url in urls:
                if (url, str(dest)) in active:
                    dupes += 1
                    continue
                active.add((url, str(dest)))
                added.append(
                    Download(
                        self._new_id(),
                        url,
                        str(dest),
                        auth,
                        dict(after),
                    )
                )
            self._enqueue(added)
        for j in added:
            self._emit(j)
        return added, dupes

    def _dedup_key(self, job: Download):
        return (job.url, job.dest_dir)

    # ------------------------------------------------------------ work
    def _reserve(self, folder: Path, name: str) -> Path:
        """Pick a free file name in ``folder`` and hold it until released."""
        p = PurePosixPath(name)
        stem, suffix = (p.stem, p.suffix) if p.suffix else (name, "")
        with self._cond:
            n = 0
            while True:
                cand = folder / (name if n == 0 else f"{stem} ({n}){suffix}")
                part = cand.with_name(cand.name + PART_SUFFIX)
                if cand not in self._reserved and not cand.exists() and not part.exists():
                    self._reserved.add(cand)
                    return cand
                n += 1

    def _release(self, path: Path) -> None:
        with self._cond:
            self._reserved.discard(path)

    def _pick_name(self, job: Download, resp) -> str:
        for candidate in (
            disposition_name(resp.headers.get("Content-Disposition")),
            urlsplit(job.url).path,
            urlsplit(resp.url or "").path,
        ):
            name = safe_name(candidate)
            if name:
                return name
        return f"download-{job.id}"

    def _run(self, job: Download) -> None:
        state: dict = {"path": None, "part": None, "validator": None, "name": None}
        try:
            while True:
                job.attempts += 1
                try:
                    self._attempt(job, state)
                    break
                except _Transient as e:
                    if job.attempts > self.retries:
                        tries = f" (gave up after {job.attempts} attempts)" if self.retries else ""
                        raise DownloadError(e.reason + tries) from e
                    delay = e.retry_after
                    if delay is None:
                        delay = self.backoff * 2 ** (job.attempts - 1)
                    delay = min(delay, MAX_DELAY)
                    job.message = f"retry {job.attempts}/{self.retries} in {delay:.0f}s: {e.reason}"
                    self._emit(job)
                    if job.cancel.wait(delay):
                        raise DownloadCancelled() from None
                    job.message = ""
        except DownloadCancelled:
            self._discard(state)
            return self._finish(job, CANCELLED, "cancelled; partial file removed")
        except HTTPError as e:
            self._discard(state)
            return self._finish(job, FAILED, f"HTTP {e.code} {e.reason}")
        except DownloadError as e:
            self._discard(state)
            return self._finish(job, FAILED, str(e))
        except (OSError, ValueError) as e:
            self._discard(state)
            return self._finish(job, FAILED, f"{type(e).__name__}: {e}")
        finally:
            if state["path"] is not None:
                self._release(state["path"])

        path = state["path"]
        job.output = str(path)
        self._finish(job, DONE, "saved")
        if self.on_saved:
            try:
                self.on_saved(job, path)
            except Exception as e:  # noqa: BLE001 - the file is saved; report the hook's failure
                job.message = f"saved; after-download step failed: {e}"

    @staticmethod
    def _discard(state: dict) -> None:
        if state["part"] is not None:
            state["part"].unlink(missing_ok=True)

    def _attempt(self, job: Download, state: dict) -> None:
        """One request. Raises _Transient for failures worth retrying; renames the .part when done."""
        url, headers = job.auth.apply(job.url)
        req = Request(url, headers={"User-Agent": USER_AGENT})
        for k, v in headers.items():
            req.add_unredirected_header(k, v)  # not re-sent on redirect
        part: Path | None = state["part"]
        have = part.stat().st_size if part is not None and part.exists() else 0
        if have and state["validator"]:
            # Continue only if the file is unchanged; otherwise the server sends it all (200).
            req.add_header("Range", f"bytes={have}-")
            req.add_header("If-Range", state["validator"])
        else:
            have = 0
        try:
            resp = urlopen(req, timeout=self.timeout)
        except HTTPError as e:
            if e.code in RETRY_STATUS:
                raise _Transient(f"HTTP {e.code} {e.reason}", _retry_after(e.headers)) from e
            if e.code == 416 and have:  # range not satisfiable: start over
                part.unlink(missing_ok=True)
                raise _Transient("server refused to continue; starting over", 0) from e
            raise
        except URLError as e:
            raise _Transient(f"cannot connect: {e.reason}") from e
        except (TimeoutError, ConnectionError, http.client.HTTPException) as e:
            raise _Transient(f"connection failed: {e}") from e

        with resp:
            if state["path"] is None:
                state["name"] = self._pick_name(job, resp)
                path = self._reserve(Path(job.dest_dir), state["name"])
                state["path"] = path
                state["part"] = part = path.with_name(path.name + PART_SUFFIX)
                job.filename = path.name
            length = int(resp.headers.get("Content-Length") or 0)
            m = re.match(r"bytes (\d+)-\d+/(\d+|\*)", resp.headers.get("Content-Range", ""))
            if have and resp.status == 206 and m and int(m.group(1)) == have:
                mode, job.done = "ab", have
                job.total = int(m.group(2)) if m.group(2) != "*" else have + length
            else:
                mode, job.done, job.total = "wb", 0, length
                etag = resp.headers.get("ETag", "")
                state["validator"] = (etag if etag and not etag.startswith("W/") else "") or resp.headers.get(
                    "Last-Modified"
                )
            try:
                with open(part, mode) as f:
                    while True:
                        chunk = resp.read(CHUNK)
                        if not chunk:
                            break
                        if job.cancel.is_set():
                            raise DownloadCancelled()
                        f.write(chunk)
                        job.done += len(chunk)
            except (TimeoutError, ConnectionError, http.client.HTTPException) as e:
                raise _Transient(f"connection dropped after {job.done} bytes: {e}") from e
        if job.total and job.done != job.total:
            raise _Transient(f"connection closed after {job.done} of {job.total} bytes")
        self._move_into_place(job, state)

    def _move_into_place(self, job: Download, state: dict) -> None:
        """Rename the finished ``.part`` to ``state["path"]`` without ever replacing an existing file.

        ``_reserve`` checked the name, but a file can still appear there before the download ends.
        If the name is taken by then, pick the next free one (``name (n).ext``) and try again.
        """
        while True:
            path: Path = state["path"]
            try:
                self._place(state["part"], path)
                return
            except FileExistsError:  # taken since _reserve: move on to the next free name
                new = self._reserve(path.parent, state["name"])
                self._release(path)
                state["path"] = new
                job.filename = new.name

    @staticmethod
    def _place(part: Path, path: Path) -> None:
        """Move ``part`` to ``path``; raise FileExistsError, never overwrite, if ``path`` exists."""
        try:
            os.link(part, path)
        except FileExistsError:
            raise
        except OSError:
            # No hard links here (drvfs /mnt/c, some network shares). Claim the name with an
            # exclusive create, then replace only that empty placeholder, which is ours.
            os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644))
            try:
                os.replace(part, path)
            except BaseException:
                path.unlink(missing_ok=True)
                raise
            return
        part.unlink(missing_ok=True)
