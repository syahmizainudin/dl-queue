# AGENTS.md

Instructions for coding agents working in this repository. Read `README.md` for what the tool does for its users.

## What this repo is

`dl-queue` downloads files from direct URLs into a folder through an in-memory queue, optionally sending an API key. A CLI (`get`) or a local web console (`ui`) drives it. It uses only the Python standard library.

It is one of three related repos:

| Repo | What it is |
|---|---|
| `dl-queue` (this one, public) | The downloader |
| `cbz-webp` (public) | The archive converter. Independent of this repo |
| `harbor` (private) | Installs both and serves them on one two-tab page. It imports `DownloadQueue`, `Auth`, `DownloadAPI` and the `on_saved`/`check_after` hooks, so treat those as public API |

| Path | What it is |
|---|---|
| `dlqueue/download.py` | `Auth`, `DownloadQueue`, file naming (`safe_name`, `disposition_name`). The only module that writes files |
| `dlqueue/basequeue.py` | `BaseQueue`: pending list, workers, pause, cancel, retry, clear |
| `dlqueue/paths.py` | `normalize_path` (accepts `C:\...` in WSL) and the `.part` suffix |
| `dlqueue/web.py` | The generic console server (`serve`, `list_dirs`, request guards), `DownloadAPI`, and `serve_downloads` for this tool's page |
| `dlqueue/ui.html` | The console page: one file, CSS and JS inline, no external requests |
| `dlqueue/cli.py` | `get` and `ui` subcommands |
| `bin/dl-queue` | Runs the CLI from a checkout (follows symlinks) |
| `tests/` | `unittest` suites. `helpers.py` has `FileServer`, a local HTTP server that checks keys, redirects, sends odd headers, stalls and cuts off |
| `ci/release_notes.py` | Release gate: tag must equal `v<__version__>`; prints that version's CHANGELOG section |
| `.github/workflows/` | `ci.yml` (lint, tests, package smoke test) and `release.yml` (tag → GitHub release) |

## Rules that keep downloads safe

These hold today, and the tests cover each one. Keep them:

1. Data goes to `<name>.part` in the destination folder and is renamed only when complete (byte count equals `Content-Length` when one was sent). Cancel, failure or a short read removes the `.part`.
2. A download never overwrites a file. `_reserve` picks `name (1).ext` and holds the name under the lock, so two concurrent downloads can't take the same name.
3. Names from `Content-Disposition` or the URL go through `safe_name`, which strips directories (no `../`) and characters Windows forbids. `filename*` wins over `filename` (RFC 6266).
4. The destination folder must exist and must not be empty text, which would mean the current directory. Only `http` and `https` URLs are accepted.
5. The API key is secret.
   - It never appears in `to_dict()`, `repr()`, messages or logs.
   - In query mode, the stored URL has no key; the key is added only when the request is made.
   - Auth headers use `add_unredirected_header`, so a redirect to another host never receives the key.
   - Header and parameter names must be RFC 9110 tokens, and keys may not contain line breaks.
   - The CLI reads the key from an environment variable or `getpass`, never from an argument.
6. `on_saved` runs only after the file is in place. An exception from it is reported in the job's message; it never deletes the file or fails the download.

If a change touches any of these, add or update a test that fails without your change.

## Testing

- Download only from `helpers.FileServer` on `127.0.0.1`. Never from a real URL in tests, and never put a real API key in a test, a fixture or a commit.
- Write only into temp folders the test creates.
- To try the console, start your own copy on a free port (`bin/dl-queue ui -p <port>`) and stop it by PID when done. Do not stop a console you did not start. Avoid `pkill -f` with a pattern that also appears in your own shell command, because it kills that shell.

```bash
python3 -m unittest discover -s tests
ruff check . && ruff format --check .
```

Both must pass before you hand work back. CI pins ruff (`RUFF_VERSION` in `ci.yml`). On Ubuntu without a venv, install it with `pip install --target <dir> ruff==<version>`. Add tests for any new behaviour: `tests/test_download.py` for the queue, `tests/test_cli_web.py` for CLI flags and HTTP endpoints, `tests/test_release_notes.py` for the release gate.

## Rules for the code

- **No dependencies.** Standard library only (`urllib`, not `requests`), and the page is plain JS with no CDN. `pyproject.toml` has `dependencies = []`; keep it that way.
- **Python 3.10+.** CI tests 3.10 through 3.14.
- **The console stays local and guarded.** It binds `127.0.0.1` only. `POST` requires `Content-Type: application/json`, a `Host` of `127.0.0.1:<port>` or `localhost:<port>`, and a same-origin `Origin` if one is sent. `GET` checks `Host` too, which stops DNS rebinding from reading `/api/state` or `/api/dirs`. New endpoints are actions on an API object (`DownloadAPI.post`), never new branches in the handler. An API raises `ValueError` for bad input, and the server turns it into a 400.
- **Nothing here knows about converting.** Tools built on this pass options through `after` (validated by their `check_after`) and act in `on_saved`. Don't add converter-specific fields to `Download`.
- **Queue state changes happen under `self._cond`.** Call `_emit` outside the lock. Workers check `job.cancel` between chunks, and `urlopen` has a timeout, so a stalled server can't block cancel forever.
- **The page must work offline and in dark mode.** Colours are CSS variables on `:root`, with a `prefers-color-scheme: dark` override. `render()` rewrites only the stats and table body every second; keep forms and the folder-picker `<dialog>` outside those. Only per-browser conveniences go in `localStorage` (save-to folder, auth mode), and the API key only when the user ticks "Remember".
- **Keep the shared code in step with cbz-webp.** This repo can't depend on cbz-webp (it would pull in Pillow), so it carries copies. harbor's tests compare the three Python copies and fail when they drift; the page markup you compare by eye. When you change one side, change the other:

  | Here | In cbz-webp |
  |---|---|
  | `class BaseQueue` in `dlqueue/basequeue.py` | `class BaseQueue` in `cbzwebp/jobqueue.py` |
  | the generic server part of `dlqueue/web.py` (between the `generic server part` markers) | the same part of `cbzwebp/web.py` |
  | `normalize_path` in `dlqueue/paths.py` | `normalize_path` in `cbzwebp/core.py` |
  | the folder-picker markup, CSS and JS in `ui.html` | the same in `cbzwebp/ui.html` and harbor's `harbor/ui.html` |

## Releasing

1. Set `__version__` in `dlqueue/__init__.py` (the only place the version lives; `pyproject.toml` reads it).
2. Move the `## Unreleased` notes in `CHANGELOG.md` under a new `## <version>` heading.
3. Commit, push to `main`, wait for CI to pass.
4. `git tag v<version> && git push origin v<version>`.

`release.yml` then checks the tag against `__version__` and the CHANGELOG, runs the full CI, builds the sdist and wheel, and publishes a GitHub release with them, a `SHA256SUMS` file and the CHANGELOG section as notes. A tag with a `-` (`v0.2.0-rc1`) becomes a pre-release. harbor pins this repo by tag, so a release is how a change reaches it. Tagging and pushing tags publishes to GitHub, so do it only when the user asks for a release.
