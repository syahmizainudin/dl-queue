# Changelog

Each release has a `## <version>` section. The release pipeline publishes that section as the GitHub release notes, and refuses to release a version without one.

## Unreleased

## 0.1.0

First release.

- `dl-queue get URL... --to DIR` downloads files through a queue and exits; `-i FILE` (or `-i -`) reads more URLs from a file or stdin.
- `dl-queue ui` serves a local web console with the same queue: add URLs, watch progress, cancel, retry, pause and resume, and pick the save-to folder with **Open folder…**.
- An API key can go as `Authorization: Bearer`, a named header or a query parameter. It is never shown in the queue or sent on redirects. The CLI reads it from an environment variable or a prompt, never from the command line.
- Downloads go to `<name>.part` and are renamed when complete, and never overwrite an existing file (`Name (1).ext`). File names come from `Content-Disposition` (including UTF-8 `filename*`) or the URL, cleaned of directories and characters Windows forbids.
- `DownloadQueue(on_saved=...)` runs a hook after each saved file, so other tools can pass downloads on; `harbor` uses it to convert archives.
