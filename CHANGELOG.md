# Changelog

Each release has a `## <version>` section. The release pipeline publishes that section as the GitHub release notes, and refuses to release a version without one.

## Unreleased

- The folder picker keeps one size; long folder lists scroll inside it instead of resizing the window.

## 0.3.0

No change to downloading. This release keeps dl-queue's copy of the console server identical to cbz-webp 0.3.0, as harbor requires: APIs may answer `GET` requests, `serve` takes extra `pages`, and pages may be framed by the same origin (`SAMEORIGIN`).

## 0.2.0

- Temporary failures are retried: timeouts, dropped connections, short reads, HTTP 5xx, 408 and 429. Backoff doubles from 1 s, honours `Retry-After` and is capped at a minute. `--retries` on `get` and `ui` and `DownloadQueue(retries=, backoff=)`; default 3. A job's `attempts` shows how many it took.
- Big files resume: the `.part` is kept between attempts and continued with `Range` + `If-Range`, so only the missing bytes are fetched. A server that ignores Range, or a file that changed, means starting over.
- `serve(..., allowed_origins=...)` accepts requests from exact extra origins, such as a browser extension. The page's own origin must now be plain `http`.

## 0.1.0

First release.

- `dl-queue get URL... --to DIR` downloads files through a queue and exits; `-i FILE` (or `-i -`) reads more URLs from a file or stdin.
- `dl-queue ui` serves a local web console with the same queue: add URLs, watch progress, cancel, retry, pause and resume, and pick the save-to folder with **Open folder…**.
- An API key can go as `Authorization: Bearer`, a named header or a query parameter. It is never shown in the queue or sent on redirects. The CLI reads it from an environment variable or a prompt, never from the command line.
- Downloads go to `<name>.part` and are renamed when complete, and never overwrite an existing file (`Name (1).ext`). File names come from `Content-Disposition` (including UTF-8 `filename*`) or the URL, cleaned of directories and characters Windows forbids.
- `DownloadQueue(on_saved=...)` runs a hook after each saved file, so other tools can pass downloads on; `harbor` uses it to convert archives.
