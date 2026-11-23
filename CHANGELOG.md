# Changelog

Each release has a `## <version>` section. The release pipeline publishes that section as the GitHub release notes, and refuses to release a version without one.

## 0.6.2

- **Keys from the host at each attempt:** `DownloadQueue(auth_for=)` and `add(..., auth_ref=)`. A download added with an `auth_ref` (a non-secret reference, at most 200 characters) asks `auth_for(job)` for its `Auth` at every attempt, retries included, so a host can keep keys out of the job and its saved state. None sends the download's own `auth`; a `ValueError` fails the download with its message. Jobs show `auth_ref` in their state, and `DownloadAPI`'s `add` takes it.

## 0.6.1

- **Pause one lane:** `set_lane_paused(name, paused)` and the API's `lane-pause` action (`{name, paused}`). A paused lane starts none of its queued downloads; running ones finish. Kept while the lane keeps its name in `set_lanes`. `download_lanes` in the state adds `paused`.
- **Taking turns between sites:** `DownloadQueue(fair=True)` and `--fair` on `get` and `ui`: when a slot frees, the next download is the one whose site (host) has the fewest downloads running in its lane, earliest queued first among equals. Off by default. The state adds `download_fair`.

## 0.6.0

- **Lanes:** `Lane(name, workers, rate, hours)`, `DownloadQueue(lanes=)` and `set_lanes()` (live) give groups of downloads their own number at once, speed limit and quiet hours; `add(..., lane=)` puts downloads in one. A lane's speed limit sits under the queue's, and a lane without hours follows the queue's. Downloads with no lane, or whose lane was removed, run as before. Jobs carry `lane`, retries keep it, and `DownloadAPI` takes `lane` on `add` and reports `download_lanes` (each lane's settings, running, queued and waiting) in its state.

## 0.5.1

- **Speed limit:** `--rate 5M` (or `DownloadQueue(rate=)`) shares one byte budget between all downloads. Cancelling still works while a download waits for its share.
- **Quiet hours:** `--hours 01:00-07:00` (or `DownloadQueue(hours=)`, see `parse_hours`): outside them no new download starts; running ones finish. The console shows "waiting until 01:00".
- **URL ranges:** `{1..20}` and `{01..20}` in a URL expand to one URL per number, in `get` and the console's add (at most 500 per add).
- `/api/state` adds `download_rate`, `download_hours` and `download_waiting`.

## 0.5.0

- **For hosts (harbor): reaching the console from other devices.** `serve()` takes `host` (bind address), `allowed_hosts` (host names accepted besides 127.0.0.1 and localhost), `ssl_context` (HTTPS; the TLS handshake runs in the request's thread) and `trust_proxy` (a reverse proxy on this machine may pass `X-Forwarded-Proto`, `-Host` and `-For`). The Origin check follows the scheme and host actually used. Without them nothing changes: the console stays on 127.0.0.1.
- `current_client()` names the client's address. An API's `get` may return `FileBody(path)` to stream a file in chunks, with single `Range` requests answered 206.
- A request that stops sending is dropped after 120 s, and handshake errors and dropped connections are no longer printed.

## 0.4.2

- A download never replaces a file that appears at its name while it runs. The finished file is moved with a hard link, or, where hard links aren't supported (such as `/mnt/c` in WSL), into a name claimed first with an exclusive create. If the name was taken, the file is saved as `name (1).ext` instead.
- Resuming works with servers that send `Content-Range: bytes N-M/*` (total size unknown) and no `Content-Length`. Before, every such resume failed the length check.
- The console page polls with one timer instead of `setInterval`: once a second while the tab is visible, every 5 seconds while it's hidden, and at once when it becomes visible again. Requests never overlap, and the table is redrawn only when something changed.
- File names are reserved under their own lock, so a slow folder no longer holds up the whole queue.

## 0.4.1

- Sign-in: the `auth` object may also define `allows(user, method, path)`. When it returns False, a signed-in request gets 403. Hosts use it for permissions on what the server serves itself, such as the folder browser (`/api/dirs`).

## 0.4.0

- The console server can require sign-in, for tools built on it (harbor's accounts): No change to downloading.
  - `serve(..., auth=...)` takes an object that says who sent each request and which requests are allowed signed out. Signed-out page requests are sent to the sign-in page, and API requests get 401.
  - `current_user()` names the signed-in user while an API handles a request.
  - An API's `post` may return extra response headers, such as `Set-Cookie`.
  Without `auth`, nothing changes.
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
