# dl-queue

[![CI](https://github.com/syahmizainudin/dl-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/syahmizainudin/dl-queue/actions/workflows/ci.yml)

`dl-queue` downloads files from direct URLs into a folder through a queue. It can send an API key with each request. You drive it from the command line or from a local web page. It needs only Python, with no other packages.

## Install

Needs Python 3.10 or later. Pick one:

```bash
pipx install git+https://github.com/syahmizainudin/dl-queue

# or from a checkout
git clone https://github.com/syahmizainudin/dl-queue.git
ln -s "$PWD/dl-queue/bin/dl-queue" ~/.local/bin/dl-queue
```

Each [release](https://github.com/syahmizainudin/dl-queue/releases) also has a wheel you can install with `pipx install dl_queue-<version>-py3-none-any.whl`.

## Commands

| Command | What it does |
|---|---|
| `dl-queue get <url>... --to <folder>` | Downloads the URLs into the folder, then exits |
| `dl-queue get -i urls.txt --to <folder>` | Reads the URLs from a file, one per line (`-i -` reads stdin; lines starting with `#` are skipped) |
| `dl-queue ui [-p 8766] [--open]` | Starts the web console at `http://127.0.0.1:8766/` |

Options for `get`:

| Option | Default | Meaning |
|---|---|---|
| `-o`, `--to` | required | Folder to save into. It must already exist |
| `--auth` | `none` | How to send the API key: `none`, `bearer`, `header` or `query` |
| `--name` | | The header name (`--auth header`, such as `X-API-Key`) or query parameter (`--auth query`, such as `api_key`) |
| `--key-env` | `DL_QUEUE_KEY` | Environment variable holding the key. If it's not set, `get` asks for the key without showing it |
| `-j`, `--jobs` | 2 | Downloads at the same time |
| `--timeout` | 60 | Seconds without data before an attempt fails |
| `--retries` | 3 | Extra attempts after a temporary failure (see below). `0` turns retrying off |
| `--rate` | none | Speed limit for all downloads together, such as `5M` or `500K` (bytes a second) |
| `--hours` | always | Start downloads only between these local times, such as `01:00-07:00` (may cross midnight). A download already running finishes |

There is no `--key` option on purpose: a key typed on the command line ends up in your shell history. Use the environment variable or the prompt.

```bash
export DL_QUEUE_KEY='...'                 # or leave it unset and type it when asked
dl-queue get https://api.example.com/files/1.zip https://api.example.com/files/2.zip \
  --to /mnt/d/Downloads --auth header --name X-API-Key
```

`get` exits with status 1 if any download failed, 2 for bad arguments, and 130 if you cancelled it with Ctrl+C. Windows paths such as `D:\Downloads` work in WSL and are read as `/mnt/d/Downloads`.

`ui` takes `-d`, `--downloads` (default 2) for the number of downloads at the same time, `--retries`, `--rate` and `--hours`.

**URL ranges:** `{1..20}` in a URL stands for 20 URLs, one per number, and `{01..20}` keeps the zero padding (`01`, `02`, …). Each URL may have one range, and one add makes at most 500 URLs. This works in `get` and in the console.

## Web console

- **File URLs:** one per line, each pointing straight at a file. `http` and `https` only.
- **Save to folder:** must already exist. **Open folder…** browses WSL's folders, with shortcuts to your home folder and each Windows drive.
- **API key sent as:** Bearer, a header you name, a query parameter you name, or no key. The key field is hidden. The page remembers the key in your browser only if you tick **Remember the key in this browser**.
- Watch progress, then cancel, retry, pause and resume, and clear finished downloads.

The queue lives in memory, so stopping the console forgets it. The console listens only on `127.0.0.1` and accepts only same-origin JSON requests, so another website cannot send it commands.

## How downloads are handled

- **File names** come from the server's `Content-Disposition` header (including UTF-8 names), or else from the URL. Folder parts such as `../` and characters Windows doesn't allow are removed.
- **Existing files are never overwritten.** A second `Chapter 01.zip` is saved as `Chapter 01 (1).zip`.
- Each file downloads to `<name>.part` and is renamed only when complete. If the server sent a size, the byte count must match it.
- **Temporary failures are retried:** timeouts, dropped connections, a download that stops short, HTTP 5xx, 408 and 429. Waits are 1 s, 2 s, 4 s and so on, or the server's `Retry-After`, capped at a minute. Other errors (401, 403, 404, ...) fail at once.
- **Big files resume.** Between attempts, the `.part` file is kept. If the server supports it, the next attempt asks only for the missing bytes (an HTTP Range request). `If-Range` with the first response's `ETag` or `Last-Modified` makes sure the file hasn't changed in between; otherwise the download starts over.
- A cancelled or finally failed download leaves nothing behind. Resuming works within one run; a restart starts the file over.
- **About the API key:**
  - It is never shown in the queue, written to disk or printed.
  - In query mode, the stored URL has no key; the key is added only when the request is made.
  - If the server redirects a download to another address (a CDN, for example), the key is not sent there.

## Using it from Python

```python
from dlqueue.download import Auth, DownloadQueue


def on_saved(job, path):  # runs after each file is saved
    print("saved", path, job.after)  # job.after holds options you passed to add()


q = DownloadQueue(workers=2, on_saved=on_saved).start()
q.add(["https://example.com/a.zip"], "/tmp/out", Auth("bearer", key), after={"convert": True})
q.wait_idle()
q.shutdown()
```

**Lanes** give groups of downloads their own limits: `DownloadQueue(lanes=[Lane("manga", workers=2, rate=parse_size("5M"), hours=parse_hours("01:00-07:00"))])`, or `q.set_lanes([...])` while it runs, and `q.add(urls, folder, lane="manga")`. A lane runs `workers` of its downloads at once, its `rate` caps the lane (the queue's `rate` still caps everything), and a lane without `hours` follows the queue's. Downloads with no lane, or whose lane was removed, run in the default lane: the queue's own `workers` and `hours`. `q.set_lane_paused("manga", True)` holds one lane's queued downloads (running ones finish); the console API's `lane-pause` action does the same. Lanes are for programs that host the queue (such as harbor); the command line doesn't set them.

**Keys from the host:** `DownloadQueue(auth_for=lambda job: Auth("bearer", key_for(job.url)))` with `q.add(urls, folder, auth_ref="site")` asks for the key at each attempt instead of storing it on the download (used by harbor's saved site keys). Returning None sends the download's own `auth`.

**Taking turns between sites:** `--fair` (or `DownloadQueue(fair=True)`) starts the next download from the site with the fewest downloads running in its lane, instead of first queued, first started. Queue 50 links from one site and 5 from another, and with 2 at once one of each runs.

`dlqueue.web.serve(port, page, apis)` and `DownloadAPI` let another tool put the download queue on its own page. [cbz-webp](https://github.com/syahmizainudin/cbz-webp) converts the downloaded archives to WebP.

## Development

```bash
python3 -m unittest discover -s tests       # tests (no network; a local test server)
ruff check . && ruff format --check .        # lint, same as CI
```

GitHub Actions runs lint, the tests on Python 3.10 to 3.14, and a package smoke test on every push to `main` and every pull request. Pushing a `v<version>` tag publishes a GitHub release; the steps are in [AGENTS.md](AGENTS.md#releasing). Changes are listed in [CHANGELOG.md](CHANGELOG.md).
