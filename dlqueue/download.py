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
    lane: str = ""  # the lane it runs in; "" is the queue's own (default) lane
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
    # Display name until the file is named; the URL doesn't change, so work it out once.
    _url_name: str = field(init=False, repr=False, compare=False, default="")

    def __post_init__(self) -> None:
        self._url_name = safe_name(urlsplit(self.url).path) or self.url

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k not in ("auth", "cancel", "_url_name")}
        d["auth_mode"] = self.auth.mode  # the key itself is never exposed
        d["name"] = self.filename or self._url_name
        return d

    def fresh(self) -> Download:
        return Download(
            self.id,
            self.url,
            self.dest_dir,
            self.auth,
            dict(self.after),
            self.lane,
        )


def parse_size(text: str) -> int:
    """Bytes per second from "0", "500K", "5M", "1.5M" or a plain number of bytes. 0 is no limit."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMG]?)(?:i?B)?(?:/s)?\s*", str(text), re.IGNORECASE)
    if not m:
        raise ValueError(f"a speed is a number with K, M or G, such as 5M: got {text!r}")
    return int(float(m.group(1)) * {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}[m.group(2).upper()])


def parse_hours(text: str) -> tuple[int, int] | None:
    """(start, end) minutes after midnight from "01:00-07:00" (may cross midnight), or None for ""."""
    if not str(text).strip():
        return None
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})\s*", str(text))
    if not m:
        raise ValueError(f"hours look like 01:00-07:00: got {text!r}")
    h1, m1, h2, m2 = (int(x) for x in m.groups())
    if h1 > 23 or h2 > 24 or m1 > 59 or m2 > 59 or (h2 == 24 and m2):
        raise ValueError(f"not a time of day in {text!r}")
    start, end = h1 * 60 + m1, h2 * 60 + m2
    if start == end:
        raise ValueError("the hours start and end at the same time")
    return start, end


def in_hours(hours: tuple[int, int] | None, now: float | None = None) -> bool:
    """Whether local time ``now`` is inside ``hours`` (always, without hours)."""
    if hours is None:
        return True
    t = time.localtime(now)
    minute = t.tm_hour * 60 + t.tm_min
    start, end = hours
    return start <= minute < end if start < end else minute >= start or minute < end % 1440


class RateLimiter:
    """One byte budget shared by every download: ``rate`` bytes a second (0: no limit), with up to a
    second's worth saved up. ``take`` waits until the bytes may be used, or the job is cancelled."""

    def __init__(self, rate: int = 0):
        self.rate = int(rate)
        self._lock = threading.Lock()
        self._allowance = 0.0
        self._at = time.monotonic()

    def take(self, n: int, cancel: threading.Event) -> bool:
        """False if ``cancel`` was set while waiting."""
        while True:
            rate = self.rate
            if rate <= 0:
                return True
            with self._lock:
                now = time.monotonic()
                self._allowance = min(self._allowance + (now - self._at) * rate, rate)
                self._at = now
                if self._allowance >= n or self._allowance >= rate:  # a chunk bigger than a second's worth
                    self._allowance -= n
                    return True
                wait = (min(n, rate) - self._allowance) / rate
            if cancel.wait(min(wait, 1.0)):
                return False


LANE_NAME_MAX = 40
LANE_WORKERS_MAX = 16
THREADS_MAX = 64  # worker threads in all; lanes past this share the threads there are


@dataclass
class Lane:
    """A named lane: at most ``workers`` of its downloads run at once, at most ``rate`` bytes a second
    for the lane (0: no limit of its own; the queue's limit still covers every download), and new
    ones start only in ``hours`` (None: the queue's hours)."""

    name: str
    workers: int = 1
    rate: int = 0
    hours: tuple[int, int] | None = None

    def __post_init__(self) -> None:
        self.name = str(self.name).strip()
        if not self.name or len(self.name) > LANE_NAME_MAX or any(ord(c) < 32 for c in self.name):
            raise ValueError(f"a lane name is 1-{LANE_NAME_MAX} characters")
        if not isinstance(self.workers, int) or isinstance(self.workers, bool):
            raise ValueError("a lane's workers must be a whole number")
        if not 1 <= self.workers <= LANE_WORKERS_MAX:
            raise ValueError(f"a lane runs 1-{LANE_WORKERS_MAX} downloads at once")
        if not isinstance(self.rate, int) or isinstance(self.rate, bool) or self.rate < 0:
            raise ValueError("a lane's speed limit must be 0 or more bytes a second")


def _site(url: str) -> str:
    """The host a URL downloads from, for ``fair`` scheduling."""
    return (urlsplit(url).hostname or "").lower()


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
        rate: int = 0,
        hours: tuple[int, int] | None = None,
        lanes: list[Lane] | None = None,
        fair: bool = False,
    ):
        """``check_after`` validates/normalizes ``after`` when jobs are added (raise ValueError).

        ``retries`` extra attempts follow a temporary failure, waiting ``backoff`` seconds, then
        twice that, and so on (or the server's ``Retry-After``), capped at a minute.

        ``rate``: bytes a second for all downloads together (0: no limit; ``parse_size``).
        ``hours``: (start, end) minutes after midnight (``parse_hours``): outside them no new
        download starts, and running ones finish.

        ``lanes``: named lanes (``Lane``), changed later with ``set_lanes``. A download added with
        no lane, or whose lane was removed, runs in the default lane: ``workers`` at once, with
        ``hours``, exactly as without lanes.

        ``fair``: when a slot frees, start the queued download whose site (host) has the fewest
        downloads running in its lane, earliest queued first among equals. Off: first queued, first
        started.
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
        self._names = threading.Lock()  # guards _reserved; keeps filesystem checks off _cond
        self.limiter = RateLimiter(rate)
        self.hours = hours
        self._lanes: dict[str, Lane] = {}
        self._lane_limiters: dict[str, RateLimiter] = {}
        self._lanes_paused: set[str] = set()
        self.fair = bool(fair)
        self._started = False
        self._worker_threads = 0
        if lanes:
            self.set_lanes(lanes)

    @property
    def rate(self) -> int:
        return self.limiter.rate

    def waiting_for_hours(self) -> bool:
        """Whether queued downloads are held back because it's outside ``hours``."""
        return not in_hours(self.hours)

    def start(self):
        super().start()
        with self._cond:
            self._started = True
            self._worker_threads = self.workers
            self._grow()
        # Workers sleep until something changes; with hours, wake them when a window may open.
        t = threading.Thread(target=self._clock, name=f"{self.thread_prefix}-clock", daemon=True)
        t.start()
        self._threads.append(t)
        return self

    def _clock(self) -> None:
        with self._cond:
            while not self._stopping:
                self._cond.wait(30)
                has_hours = self.hours is not None or any(x.hours is not None for x in self._lanes.values())
                if has_hours and self._pending:
                    self._cond.notify_all()

    # ------------------------------------------------------------ lanes
    def set_lanes(self, lanes: list[Lane]) -> None:
        """Replace the lanes, live. A lane that keeps its name keeps its byte budget; a running
        download keeps running. Queued downloads of a removed lane run in the default lane."""
        lanes = list(lanes)
        if any(not isinstance(x, Lane) for x in lanes):
            raise ValueError("lanes must be Lane objects")
        names = [x.name.lower() for x in lanes]
        if len(set(names)) != len(names):
            raise ValueError("lane names must be different")
        with self._cond:
            self._lanes = {x.name: x for x in lanes}
            limiters = {}
            for x in lanes:
                limiters[x.name] = self._lane_limiters.get(x.name) or RateLimiter()
                limiters[x.name].rate = x.rate
            self._lane_limiters = limiters
            self._lanes_paused &= set(self._lanes)
            if self._started:
                self._grow()
            self._cond.notify_all()

    def set_lane_paused(self, name: str, paused: bool) -> None:
        """Pause or resume one lane: while paused none of its queued downloads start; running ones
        finish. Kept while the lane keeps its name in ``set_lanes``."""
        name = str(name or "").strip()
        with self._cond:
            if name not in self._lanes:
                raise ValueError(f"there is no download lane {name!r}")
            if paused:
                self._lanes_paused.add(name)
            else:
                self._lanes_paused.discard(name)
            self._cond.notify_all()

    def lanes(self) -> list[Lane]:
        with self._cond:
            return list(self._lanes.values())

    def _grow(self) -> None:
        """Start worker threads up to the default lane's workers plus every lane's. Call with the lock
        held. Threads aren't stopped when lanes shrink: ``_next_runnable`` counts each lane's running
        downloads, so a spare thread just waits."""
        want = min(THREADS_MAX, self.workers + sum(x.workers for x in self._lanes.values()))
        while self._worker_threads < want:
            n = self._worker_threads
            t = threading.Thread(target=self._worker, name=f"{self.thread_prefix}-{n}", daemon=True)
            t.start()
            self._threads.append(t)
            self._worker_threads += 1

    def _lane(self, job: Download) -> Lane | None:
        """The job's lane, or None for the default lane (no lane, or a lane since removed)."""
        return self._lanes.get(job.lane) if job.lane else None

    def lanes_state(self) -> list[dict]:
        """Each lane with its settings and how many of its downloads run and wait."""
        with self._cond:
            out = []
            for x in self._lanes.values():
                jobs = [j for j in self._jobs.values() if j.lane == x.name]
                hours = x.hours if x.hours is not None else self.hours
                out.append(
                    {
                        "name": x.name,
                        "workers": x.workers,
                        "rate": x.rate,
                        "hours": x.hours,
                        "running": sum(j.state == "running" for j in jobs),
                        "queued": sum(j.state == QUEUED for j in jobs),
                        "waiting": not in_hours(hours),
                        "paused": x.name in self._lanes_paused,
                    }
                )
            return out

    def _next_runnable(self):
        """First pending job whose lane has a free slot, isn't paused and is inside its hours; with
        ``fair``, the one whose site has the fewest running downloads in its lane. Call with the lock
        held."""
        running: dict[str, int] = {}
        per_site: dict[tuple[str, str], int] = {}
        busy = set()
        for j in self._jobs.values():
            if j.state == "running":
                lane = self._lane(j)
                key = lane.name if lane else ""
                running[key] = running.get(key, 0) + 1
                site = (key, _site(j.url))
                per_site[site] = per_site.get(site, 0) + 1
                busy.add(self._busy_key(j))
        busy.discard(None)
        best, best_load = None, None
        for jid in self._pending:
            job = self._jobs[jid]
            lane = self._lane(job)
            key, workers = (lane.name, lane.workers) if lane else ("", self.workers)
            hours = lane.hours if lane and lane.hours is not None else self.hours
            if (
                running.get(key, 0) >= workers
                or (lane and lane.name in self._lanes_paused)
                or not in_hours(hours)
                or self._busy_key(job) in busy
            ):
                continue
            if not self.fair:
                best = jid
                break
            load = per_site.get((key, _site(job.url)), 0)
            if best_load is None or load < best_load:
                best, best_load = jid, load
                if load == 0:
                    break  # nothing beats a site with nothing running
        if best is None:
            return None
        self._pending.remove(best)
        return self._jobs[best]

    def _take(self, job: Download, n: int) -> bool:
        """Wait for ``n`` bytes of the lane's budget, then the queue's. False if cancelled."""
        lane = self._lane_limiters.get(job.lane) if job.lane else None
        if lane is not None and not lane.take(n, job.cancel):
            return False
        return self.limiter.take(n, job.cancel)

    # ------------------------------------------------------------ commands
    def add(
        self,
        urls: list[str],
        dest_dir: str | Path,
        auth: Auth | None = None,
        *,
        after: dict | None = None,
        lane: str = "",
    ) -> tuple[list[Download], int]:
        """Queue each URL not already queued/running for the same folder, in ``lane`` ("" for the
        default lane). Returns (added, dupes)."""
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
        lane = str(lane or "").strip()
        added, dupes = [], 0
        with self._cond:
            if lane and lane not in self._lanes:
                raise ValueError(f"there is no download lane {lane!r}")
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
                        lane,
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
        with self._names:  # the check and the add under one lock
            n = 0
            while True:
                cand = folder / (name if n == 0 else f"{stem} ({n}){suffix}")
                part = cand.with_name(cand.name + PART_SUFFIX)
                if cand not in self._reserved and not cand.exists() and not part.exists():
                    self._reserved.add(cand)
                    return cand
                n += 1

    def _release(self, path: Path) -> None:
        with self._names:
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
        except (DownloadCancelled, DownloadError, OSError, ValueError) as e:
            self._discard(state)
            return self._finish(job, *self._failure(e))
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
    def _failure(e: Exception) -> tuple[str, str]:
        """The final state and message for an exception that ended a download."""
        if isinstance(e, DownloadCancelled):
            return CANCELLED, "cancelled; partial file removed"
        if isinstance(e, HTTPError):  # before OSError: HTTPError is a subclass of it
            return FAILED, f"HTTP {e.code} {e.reason}"
        if isinstance(e, DownloadError):
            return FAILED, str(e)
        return FAILED, f"{type(e).__name__}: {e}"

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
            m = re.match(r"bytes (\d+)-(\d+)/(\d+|\*)", resp.headers.get("Content-Range", ""))
            if have and resp.status == 206 and m and int(m.group(1)) == have:
                mode, job.done = "ab", have
                # Total size: the range's "/total", or its last byte + 1 when the total is "*".
                job.total = int(m.group(3)) if m.group(3) != "*" else int(m.group(2)) + 1
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
                        if job.cancel.is_set() or not self._take(job, len(chunk)):
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
