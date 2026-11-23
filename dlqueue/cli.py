"""Command line: ``dl-queue get`` (download URLs, then exit) and ``dl-queue ui``."""

from __future__ import annotations

import argparse
import getpass
import os
import signal
import sys
import threading
import webbrowser

from . import __version__
from .basequeue import CANCELLED, DONE, FAILED, RUNNING
from .download import AUTH_MODES, Auth, Download, DownloadQueue, parse_hours, parse_size
from .web import expand_ranges

DEFAULT_KEY_ENV = "DL_QUEUE_KEY"


def _fmt(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="dl-queue",
        description="Download files from direct URLs through a queue, with an optional API key.",
    )
    ap.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("get", help="download URLs into a folder, then exit")
    g.add_argument("urls", nargs="*", help="file URLs (http/https)")
    g.add_argument("-i", "--input", help="read more URLs from this file, one per line ('-' for stdin)")
    g.add_argument("-o", "--to", required=True, help="folder to save into (must exist)")
    g.add_argument(
        "--auth",
        choices=AUTH_MODES,
        default="none",
        help="how to send the API key: none (default), bearer, header, query",
    )
    g.add_argument("--name", default="", help="header name (--auth header) or query parameter (--auth query)")
    g.add_argument(
        "--key-env",
        default=DEFAULT_KEY_ENV,
        help=f"environment variable holding the API key (default {DEFAULT_KEY_ENV}); "
        "if unset, you are asked for it",
    )
    g.add_argument("-j", "--jobs", type=int, default=2, help="downloads at the same time (default 2)")
    g.add_argument(
        "--timeout", type=float, default=60, help="seconds without data before giving up (default 60)"
    )
    g.add_argument(
        "--retries", type=int, default=3, help="extra attempts after a temporary failure (default 3)"
    )

    u = sub.add_parser("ui", help="start the local web console")
    u.add_argument("-p", "--port", type=int, default=8766, help="port on 127.0.0.1 (default 8766)")
    u.add_argument("-d", "--downloads", type=int, default=2, help="downloads at the same time (default 2)")
    u.add_argument(
        "--retries", type=int, default=3, help="extra attempts after a temporary failure (default 3)"
    )
    u.add_argument("--open", action="store_true", help="open the console in a browser")
    for p in (g, u):
        p.add_argument(
            "--rate",
            default="0",
            help="speed limit for all downloads together, such as 5M or 500K (default none)",
        )
        p.add_argument(
            "--hours", default="", help="start downloads only between these times, such as 01:00-07:00"
        )
        p.add_argument(
            "--fair",
            action="store_true",
            help="take turns between sites: start the next download from the site with the fewest running",
        )
    return ap


def _read_urls(args) -> list[str]:
    urls = list(args.urls)
    if args.input:
        f = sys.stdin if args.input == "-" else open(args.input, encoding="utf-8")
        with f:
            urls += [ln.strip() for ln in f if ln.strip() and not ln.lstrip().startswith("#")]
    return urls


def _key(args) -> str:
    """The key comes from an environment variable or a prompt, never the command line (shell history)."""
    if args.auth == "none":
        return ""
    key = os.environ.get(args.key_env, "")
    if key:
        return key
    if sys.stdin.isatty():
        return getpass.getpass(f"API key (not shown; set {args.key_env} to skip this): ")
    raise ValueError(f"--auth {args.auth} needs a key: set {args.key_env}")


def cmd_get(args) -> int:
    try:
        urls = expand_ranges(_read_urls(args))
        rate, hours = parse_size(args.rate), parse_hours(args.hours)
        if args.jobs < 1:
            raise ValueError("--jobs must be at least 1")
        if args.retries < 0:
            raise ValueError("--retries must not be negative")
        auth = Auth(args.auth, _key(args), args.name)
    except (OSError, ValueError) as e:
        print(f"dl-queue: {e}", file=sys.stderr)
        return 2

    lock = threading.Lock()
    finished = [0]
    total = [0]

    def on_change(job: Download) -> None:
        with lock:
            if job.state == RUNNING:
                if not job.message:
                    print(f"[start] {job.url}", flush=True)
                elif job.message.startswith("retry"):
                    print(f"[{job.message}] {job.url}", flush=True)
                return
            if job.state in (DONE, FAILED, CANCELLED):
                finished[0] += 1
                tag = {DONE: "done", FAILED: "FAIL"}.get(job.state, "cancel")
                where = f" -> {job.output} ({_fmt(job.done)})" if job.state == DONE else ""
                print(f"[{tag} {finished[0]}/{total[0]}] {job.url}{where} {job.message}", flush=True)

    q = DownloadQueue(
        workers=args.jobs,
        timeout=args.timeout,
        retries=args.retries,
        on_change=on_change,
        rate=rate,
        hours=hours,
        fair=args.fair,
    )
    q.set_paused(True)  # queue everything first, so the progress count is right
    q.start()
    try:
        added, dupes = q.add(urls, args.to, auth)
    except ValueError as e:
        q.shutdown()
        print(f"dl-queue: {e}", file=sys.stderr)
        return 2
    total[0] = len(added)
    if dupes:
        print(f"Skipped {dupes} duplicate URL(s).")

    def interrupt(*_):
        print("\nCancelling… (partial files are removed)", flush=True)
        q.cancel_all()

    signal.signal(signal.SIGINT, interrupt)
    q.set_paused(False)
    q.wait_idle()
    q.shutdown()

    jobs = q.jobs()
    done = [j for j in jobs if j.state == DONE]
    failed = [j for j in jobs if j.state == FAILED]
    cancelled = [j for j in jobs if j.state == CANCELLED]
    print(
        f"\n{len(done)} downloaded ({_fmt(sum(j.done for j in done))}), "
        f"{len(failed)} failed, {len(cancelled)} cancelled."
    )
    if failed:
        return 1
    return 130 if cancelled else 0


def cmd_ui(args) -> int:
    from .web import serve_downloads

    if args.downloads < 1 or args.retries < 0:
        print("dl-queue: --downloads must be at least 1 and --retries not negative", file=sys.stderr)
        return 2
    try:
        rate, hours = parse_size(args.rate), parse_hours(args.hours)
    except ValueError as e:
        print(f"dl-queue: {e}", file=sys.stderr)
        return 2
    q = DownloadQueue(
        workers=args.downloads, retries=args.retries, rate=rate, hours=hours, fair=args.fair
    ).start()
    try:
        server = serve_downloads(args.port, q)
    except OSError as e:
        print(f"dl-queue: cannot listen on 127.0.0.1:{args.port}: {e}", file=sys.stderr)
        q.shutdown()
        return 2
    url = f"http://127.0.0.1:{args.port}/"
    print(f"dl-queue console: {url}  (Ctrl+C to stop; running downloads are cancelled)", flush=True)

    def stop(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    if args.open:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping…", flush=True)
    finally:
        server.server_close()
        q.shutdown()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return cmd_get(args) if args.cmd == "get" else cmd_ui(args)


if __name__ == "__main__":
    sys.exit(main())
