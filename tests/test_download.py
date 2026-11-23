"""Tests for the download queue, against a local HTTP server (never a real URL)."""

import errno
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from helpers import FileServer

from dlqueue.basequeue import CANCELLED, DONE, FAILED
from dlqueue.download import Auth, DownloadQueue, Lane, disposition_name, safe_name

KEY = FileServer.KEY


class DownloadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.server = FileServer()
        self.queues = []

    def tearDown(self):
        for q in self.queues:
            q.shutdown()
        self.server.close()
        self.tmp.cleanup()

    def queue(self, workers=1, **kw):
        kw.setdefault("retries", 0)  # tests that want retries ask for them
        kw.setdefault("backoff", 0.01)
        q = DownloadQueue(workers=workers, timeout=10, **kw).start()
        self.queues.append(q)
        return q

    def run_one(self, path, auth=None, **kw):
        q = self.queue()
        (job,), _ = q.add([self.server.base + path], self.dir, auth, **kw)
        q.wait_idle()
        return job, q

    def files(self):
        return sorted(p.name for p in self.dir.iterdir())

    def last_request(self, prefix):
        return [r for r in self.server.requests if r["path"].startswith(prefix)][-1]

    # ---------------------------------------------------------- auth modes
    def test_bearer(self):
        job, _ = self.run_one("/secure/a.bin", Auth("bearer", KEY))
        self.assertEqual(job.state, DONE, job.message)
        self.assertEqual((self.dir / "a.bin").read_bytes(), b"data:a.bin")
        self.assertEqual(self.last_request("/secure")["headers"]["Authorization"], f"Bearer {KEY}")
        self.assertEqual(self.files(), ["a.bin"])

    def test_custom_header(self):
        job, _ = self.run_one("/secure/a.bin", Auth("header", KEY, "X-API-Key"))
        self.assertEqual(job.state, DONE, job.message)
        self.assertNotIn("Authorization", self.last_request("/secure")["headers"])

    def test_query_param_key_not_stored(self):
        job, _ = self.run_one("/secure/a.bin?x=1", Auth("query", KEY, "api_key"))
        self.assertEqual(job.state, DONE, job.message)
        req = self.last_request("/secure")
        self.assertEqual(req["query"], {"x": ["1"], "api_key": [KEY]})
        self.assertNotIn(KEY, job.url)

    def test_key_never_in_state(self):
        for auth in (Auth("bearer", KEY), Auth("header", KEY, "X-Token"), Auth("query", KEY, "k")):
            _, q = self.run_one("/secure/a.bin", auth)
            self.assertNotIn(KEY, json.dumps(q.snapshot()))
            self.assertNotIn(KEY, repr(q.jobs()))

    def test_wrong_key_fails(self):
        job, _ = self.run_one("/secure/a.bin", Auth("bearer", "wrong"))
        self.assertEqual(job.state, FAILED)
        self.assertIn("HTTP 401", job.message)
        self.assertEqual(self.files(), [])

    def test_404_fails(self):
        job, _ = self.run_one("/missing")
        self.assertEqual(job.state, FAILED)
        self.assertIn("HTTP 404", job.message)

    def test_redirect_does_not_forward_key(self):
        job, _ = self.run_one("/redirect", Auth("bearer", KEY))
        self.assertEqual(job.state, DONE, job.message)
        self.assertEqual(self.last_request("/redirect")["headers"]["Authorization"], f"Bearer {KEY}")
        landing = self.last_request("/landing")["headers"]
        self.assertNotIn("Authorization", landing)
        self.assertEqual((self.dir / "redirect").read_bytes(), b"landed")

    def test_unreachable_server_fails(self):
        q = self.queue()
        (job,), _ = q.add(["http://127.0.0.1:1/x.zip"], self.dir)
        q.wait_idle()
        self.assertEqual(job.state, FAILED)
        self.assertIn("cannot connect", job.message)

    # ---------------------------------------------------------- file names
    def test_content_disposition_is_sanitized(self):
        job, _ = self.run_one("/cd?name=../../evil%20name.zip")
        self.assertEqual(job.state, DONE, job.message)
        self.assertEqual(self.files(), ["evil name.zip"])
        self.assertFalse((self.dir.parent / "evil name.zip").exists())

    def test_rfc5987_filename(self):
        job, _ = self.run_one("/cdstar")
        self.assertEqual(job.filename, "Volé 01.zip")

    def test_existing_file_not_overwritten(self):
        (self.dir / "a.bin").write_bytes(b"mine")
        job, _ = self.run_one("/files/x/a.bin")
        self.assertEqual(job.filename, "a (1).bin")
        self.assertEqual((self.dir / "a.bin").read_bytes(), b"mine")

    def _file_appears_after_reserve(self, q, content=b"theirs"):
        """Make a file appear at the reserved name while the download is running."""
        reserve = q._reserve

        def reserve_then_take(folder, name):
            path = reserve(folder, name)
            if not (folder / name).exists():  # first reservation only
                (folder / name).write_bytes(content)
            return path

        q._reserve = reserve_then_take

    def test_file_appearing_during_download_not_overwritten(self):
        q = self.queue()
        self._file_appears_after_reserve(q)
        (job,), _ = q.add([self.server.base + "/files/x/a.bin"], self.dir)
        q.wait_idle()
        self.assertEqual(job.state, DONE, job.message)
        self.assertEqual((self.dir / "a.bin").read_bytes(), b"theirs")
        self.assertEqual(job.filename, "a (1).bin")
        self.assertEqual(job.output, str(self.dir / "a (1).bin"))
        self.assertEqual((self.dir / "a (1).bin").read_bytes(), b"data:a.bin")
        self.assertEqual(self.files(), ["a (1).bin", "a.bin"])

    def test_no_hard_links_still_never_overwrites(self):
        q = self.queue()
        self._file_appears_after_reserve(q)
        with mock.patch("dlqueue.download.os.link", side_effect=OSError(errno.EPERM, "no links")) as link:
            (job,), _ = q.add([self.server.base + "/files/x/a.bin"], self.dir)
            q.wait_idle()
        self.assertTrue(link.called)
        self.assertEqual(job.state, DONE, job.message)
        self.assertEqual((self.dir / "a.bin").read_bytes(), b"theirs")
        self.assertEqual(job.filename, "a (1).bin")
        self.assertEqual((self.dir / "a (1).bin").read_bytes(), b"data:a.bin")
        self.assertEqual(self.files(), ["a (1).bin", "a.bin"])

    def test_no_hard_links_saves_normally(self):
        with mock.patch("dlqueue.download.os.link", side_effect=OSError(errno.EPERM, "no links")):
            job, _ = self.run_one("/files/x/a.bin")
        self.assertEqual(job.state, DONE, job.message)
        self.assertEqual((self.dir / "a.bin").read_bytes(), b"data:a.bin")
        self.assertEqual(self.files(), ["a.bin"])

    def test_concurrent_same_name_get_distinct_files(self):
        q = self.queue(workers=3)
        urls = [f"{self.server.base}/files/{d}/same.bin" for d in "abc"]
        q.add(urls, self.dir)
        q.wait_idle()
        self.assertEqual(self.files(), ["same (1).bin", "same (2).bin", "same.bin"])

    # ---------------------------------------------------------- partial data
    def test_no_content_length(self):
        job, _ = self.run_one("/nolength")
        self.assertEqual(job.state, DONE, job.message)
        self.assertEqual(job.total, 0)
        self.assertEqual((self.dir / "nolength").read_bytes(), b"no length here")

    def test_short_read_fails_and_cleans_up(self):
        job, _ = self.run_one("/short")
        self.assertEqual(job.state, FAILED)
        self.assertEqual(self.files(), [])

    def test_cancel_mid_download_cleans_up(self):
        q = self.queue()
        (job,), _ = q.add([self.server.base + "/slow"], self.dir)
        deadline = time.time() + 10
        while job.done < 256 * 1024 and time.time() < deadline:
            time.sleep(0.05)
        self.assertTrue(q.cancel(job.id))
        q.wait_idle()
        self.assertEqual(job.state, CANCELLED)
        self.assertEqual(self.files(), [])

    def test_retry_after_failure(self):
        job, q = self.run_one("/secure/a.bin", Auth("bearer", "wrong"))
        self.assertTrue(q.retry(job.id))
        q.wait_idle()
        self.assertEqual(q.jobs()[0].state, FAILED)  # same key, same result
        self.assertFalse(q.retry(999))

    # ---------------------------------------------------------- retries and resume
    def test_retries_then_succeeds(self):
        q = self.queue(retries=3)
        (job,), _ = q.add([self.server.base + "/flaky/2/a.bin"], self.dir)
        q.wait_idle()
        self.assertEqual((job.state, job.attempts), (DONE, 3), job.message)
        self.assertEqual((self.dir / "a.bin").read_bytes(), b"data:a.bin")

    def test_gives_up_after_retries(self):
        q = self.queue(retries=2)
        (job,), _ = q.add([self.server.base + "/always500"], self.dir)
        q.wait_idle()
        self.assertEqual((job.state, job.attempts), (FAILED, 3))
        self.assertIn("HTTP 500", job.message)
        self.assertIn("gave up after 3 attempts", job.message)
        self.assertEqual(self.server.counts["/always500"], 3)
        self.assertEqual(self.files(), [])

    def test_client_errors_not_retried(self):
        q = self.queue(retries=3)
        (job,), _ = q.add([self.server.base + "/secure/a.bin"], self.dir, Auth("bearer", "wrong"))
        q.wait_idle()
        self.assertEqual((job.state, job.attempts), (FAILED, 1))
        self.assertEqual(self.server.counts["/secure/a.bin"], 1)

    def test_retry_after_is_honoured(self):
        q = self.queue(retries=1, backoff=0)
        (job,), _ = q.add([self.server.base + "/busy"], self.dir)
        start = time.monotonic()
        q.wait_idle()
        self.assertEqual(job.state, DONE, job.message)
        self.assertGreaterEqual(time.monotonic() - start, 0.9)

    def test_resume_continues_partial_file(self):
        q = self.queue(retries=2)
        (job,), _ = q.add([self.server.base + "/resume/big.bin"], self.dir)
        q.wait_idle()
        self.assertEqual((job.state, job.attempts), (DONE, 2), job.message)
        self.assertEqual((self.dir / "big.bin").read_bytes(), FileServer.RESUME_BODY)
        second = [r for r in self.server.requests if r["path"] == "/resume/big.bin"][1]["headers"]
        half = len(FileServer.RESUME_BODY) // 2
        self.assertEqual((second.get("Range"), second.get("If-Range")), (f"bytes={half}-", '"v1"'))
        self.assertEqual(self.files(), ["big.bin"])

    def test_resume_without_total_size(self):
        # 206 with Content-Range "bytes N-M/*" and no Content-Length: the size comes from M.
        q = self.queue(retries=2)
        (job,), _ = q.add([self.server.base + "/resumestar/big.bin"], self.dir)
        q.wait_idle()
        self.assertEqual((job.state, job.attempts), (DONE, 2), job.message)
        self.assertEqual(job.total, len(FileServer.RESUME_BODY))
        self.assertEqual((self.dir / "big.bin").read_bytes(), FileServer.RESUME_BODY)
        self.assertEqual(self.files(), ["big.bin"])

    def test_server_ignoring_range_restarts_cleanly(self):
        q = self.queue(retries=2)
        (job,), _ = q.add([self.server.base + "/noresume/big.bin"], self.dir)
        q.wait_idle()
        self.assertEqual(job.state, DONE, job.message)
        self.assertEqual((self.dir / "big.bin").read_bytes(), FileServer.RESUME_BODY)  # not doubled

    def test_cancel_during_backoff_is_quick_and_cleans_up(self):
        q = self.queue(retries=5, backoff=30)
        (job,), _ = q.add([self.server.base + "/resume/big.bin"], self.dir)
        deadline = time.time() + 10
        while not job.message.startswith("retry") and time.time() < deadline:
            time.sleep(0.05)
        self.assertTrue(job.message.startswith("retry 1/5 in 30s"), job.message)
        self.assertTrue(list(self.dir.glob("*.part")))  # kept for the next attempt
        start = time.monotonic()
        q.cancel(job.id)
        q.wait_idle()
        self.assertLess(time.monotonic() - start, 2)
        self.assertEqual(job.state, CANCELLED)
        self.assertEqual(self.files(), [])

    def test_negative_settings_rejected(self):
        with self.assertRaises(ValueError):
            DownloadQueue(retries=-1)

    # ---------------------------------------------------------- after-download hook
    def test_on_saved_hook_gets_job_and_path(self):
        seen = []

        def hook(job, path):
            seen.append((job.after, path.name, path.read_bytes()))
            job.linked_id = 42
            job.message = "saved; handed on"

        q = self.queue(on_saved=hook)
        (job,), _ = q.add([self.server.base + "/files/x/a.zip"], self.dir, after={"convert": True})
        q.wait_idle()
        self.assertEqual(seen, [({"convert": True}, "a.zip", b"data:a.zip")])
        self.assertEqual((job.state, job.linked_id, job.message), (DONE, 42, "saved; handed on"))
        self.assertEqual(q.snapshot()[0]["after"], {"convert": True})

    def test_hook_not_called_on_failure(self):
        seen = []
        q = self.queue(on_saved=lambda job, path: seen.append(path))
        q.add([self.server.base + "/missing"], self.dir)
        q.wait_idle()
        self.assertEqual(seen, [])

    def test_hook_error_keeps_file(self):
        def hook(job, path):
            raise RuntimeError("converter is down")

        q = self.queue(on_saved=hook)
        (job,), _ = q.add([self.server.base + "/files/x/a.bin"], self.dir)
        q.wait_idle()
        self.assertEqual(job.state, DONE)
        self.assertIn("after-download step failed: converter is down", job.message)
        self.assertEqual(self.files(), ["a.bin"])

    def test_check_after_validates_on_add(self):
        def check(after):
            if after.get("quality", 80) > 100:
                raise ValueError("quality must be 1-100")
            return {"quality": 80, **after}

        q = self.queue(check_after=check)
        with self.assertRaisesRegex(ValueError, "quality"):
            q.add([self.server.base + "/files/x/a.bin"], self.dir, after={"quality": 101})
        q.set_paused(True)
        (job,), _ = q.add([self.server.base + "/files/x/a.bin"], self.dir)
        self.assertEqual(job.after, {"quality": 80})

    def test_retry_keeps_after(self):
        q = self.queue()
        (job,), _ = q.add([self.server.base + "/missing"], self.dir, after={"x": 1})
        q.wait_idle()
        self.assertTrue(q.retry(job.id))
        self.assertEqual(q.jobs()[0].after, {"x": 1})

    # ---------------------------------------------------------- name helpers
    def test_safe_name(self):
        self.assertEqual(safe_name("../../evil name.zip"), "evil name.zip")
        self.assertEqual(safe_name("..\\x\\..hidden"), "hidden")
        self.assertEqual(safe_name("a%2Fb.zip"), "b.zip")
        self.assertEqual(safe_name('a<b>:c"d|e?f*.zip'), "a_b__c_d_e_f_.zip")
        self.assertEqual(safe_name("..."), "")

    def test_disposition_name(self):
        self.assertEqual(disposition_name('attachment; filename="a b.zip"'), "a b.zip")
        self.assertEqual(
            disposition_name("attachment; filename=\"x.zip\"; filename*=UTF-8''Vol%C3%A9%2001.zip"),
            "Volé 01.zip",
        )
        self.assertEqual(disposition_name(None), "")
        self.assertEqual(disposition_name("inline"), "")

    # ---------------------------------------------------------- validation
    def test_validation(self):
        q = self.queue()
        bad = [
            (["ftp://x/y.zip"], self.dir, None),
            (["file:///etc/passwd"], self.dir, None),
            (["not a url"], self.dir, None),
            ([], self.dir, None),
            (["http://x/y"], self.dir / "missing", None),
            (["http://x/y"], "", None),
            (["http://x/y"], "  ", None),
            (["http://x/y"], self.dir, Auth("bearer", "")),
            (["http://x/y"], self.dir, Auth("bearer", "a\r\nX-Evil: 1")),
            (["http://x/y"], self.dir, Auth("header", KEY, "Bad:Name")),
            (["http://x/y"], self.dir, Auth("header", KEY, "")),
            (["http://x/y"], self.dir, Auth("query", KEY, "a b")),
            (["http://x/y"], self.dir, Auth("basic", KEY)),
        ]
        for urls, dest, auth in bad:
            with self.subTest(urls=urls, dest=dest, auth=auth), self.assertRaises(ValueError):
                q.add(urls, dest, auth)
        self.assertEqual(q.jobs(), [])

    def test_dedup_same_url_same_folder(self):
        q = self.queue()
        q.set_paused(True)
        url = self.server.base + "/files/x/a.bin"
        self.assertEqual(len(q.add([url, url], self.dir)[0]), 1)
        self.assertEqual(q.add([url], self.dir)[1], 1)
        other = self.dir / "o"
        other.mkdir()
        self.assertEqual(len(q.add([url], other)[0]), 1)

    def test_pause_holds_work(self):
        q = self.queue()
        q.set_paused(True)
        (job,), _ = q.add([self.server.base + "/files/x/a.bin"], self.dir)
        time.sleep(0.3)
        self.assertEqual(job.state, "queued")
        q.set_paused(False)
        q.wait_idle()
        self.assertEqual(job.state, DONE)

    def test_shutdown_cancels_running(self):
        q = DownloadQueue(workers=1, timeout=10).start()
        (job,), _ = q.add([self.server.base + "/slow"], self.dir)
        while job.done == 0:
            time.sleep(0.05)
        t = threading.Thread(target=q.shutdown)
        t.start()
        t.join(10)
        self.assertFalse(t.is_alive())
        self.assertEqual(job.state, CANCELLED)
        self.assertEqual(self.files(), [])


class SpeedAndHoursTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.server = FileServer({"big.bin": b"x" * 300_000})
        self.q = None

    def tearDown(self):
        if self.q:
            self.q.shutdown()
        self.server.close()
        self.tmp.cleanup()

    def test_parsing(self):
        from dlqueue.download import parse_hours, parse_size

        self.assertEqual(
            [parse_size(x) for x in ("0", "500", "500K", "5M", "1.5m", "2MB/s", "1G")],
            [0, 500, 512000, 5242880, 1572864, 2097152, 1073741824],
        )
        for bad in ("fast", "-1", "5T", ""):
            with self.assertRaises(ValueError):
                parse_size(bad)
        self.assertEqual(parse_hours("01:00-07:30"), (60, 450))
        self.assertEqual(parse_hours("22:00-06:00"), (1320, 360))
        self.assertEqual(parse_hours("00:00-24:00"), (0, 1440))
        self.assertIsNone(parse_hours(""))
        for bad in ("1-7", "25:00-01:00", "01:00-01:00", "10:61-11:00", "00:00-24:30"):
            with self.assertRaises(ValueError):
                parse_hours(bad)

    def test_in_hours(self):
        from dlqueue.download import in_hours

        def at(h, m=0):
            return time.mktime((2026, 11, 19, h, m, 0, 0, 0, -1))

        self.assertTrue(in_hours(None, at(12)))
        self.assertTrue(in_hours((60, 420), at(3)))
        self.assertFalse(in_hours((60, 420), at(7)))
        self.assertTrue(in_hours((1320, 360), at(23)))  # across midnight
        self.assertTrue(in_hours((1320, 360), at(2)))
        self.assertFalse(in_hours((1320, 360), at(12)))

    def test_speed_limit(self):
        self.q = DownloadQueue(workers=2, timeout=10, retries=0, rate=200_000).start()
        start = time.monotonic()
        self.q.add([self.server.base + "/files/a/big.bin", self.server.base + "/files/b/big.bin"], self.dir)
        self.q.wait_idle()
        took = time.monotonic() - start
        self.assertEqual(sorted(j.state for j in self.q.jobs()), [DONE, DONE])
        # 600 KB at 200 KB/s with up to a second saved up: at least ~2 s, both downloads sharing it
        self.assertGreater(took, 1.8)
        self.assertEqual((self.dir / "big.bin").stat().st_size, 300_000)

    def test_cancel_while_waiting_for_the_limit(self):
        self.q = DownloadQueue(workers=1, timeout=10, retries=0, rate=10_000).start()
        (job,), _ = self.q.add([self.server.base + "/files/a/big.bin"], self.dir)
        deadline = time.monotonic() + 5
        while job.state != "running" and time.monotonic() < deadline:
            time.sleep(0.02)
        time.sleep(0.3)
        self.q.cancel(job.id)
        self.q.wait_idle()
        self.assertEqual(job.state, CANCELLED)
        self.assertEqual(list(self.dir.iterdir()), [])  # the .part is gone

    def test_quiet_hours_hold_new_downloads(self):
        now = time.localtime()
        minute = now.tm_hour * 60 + now.tm_min
        closed = ((minute + 60) % 1440, (minute + 120) % 1440)  # an hour from now, for an hour
        self.q = DownloadQueue(workers=1, timeout=10, retries=0, hours=closed).start()
        (job,), _ = self.q.add([self.server.base + "/files/a/x.bin"], self.dir)
        time.sleep(0.5)
        self.assertEqual(job.state, "queued")
        self.assertTrue(self.q.waiting_for_hours())
        self.q.hours = None  # the window opens: the clock wakes the workers within 30 s; poke now
        with self.q._cond:
            self.q._cond.notify_all()
        self.q.wait_idle()
        self.assertEqual(job.state, DONE)


class LaneTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.server = FileServer()
        self.q = None

    def tearDown(self):
        if self.q:
            self.q.shutdown()
        self.server.close()
        self.tmp.cleanup()

    def folder(self, name):
        d = self.dir / name
        d.mkdir()
        return d

    def slow(self, folder, lane=""):
        (job,), _ = self.q.add([self.server.base + "/slow"], self.folder(folder), lane=lane)
        return job

    def wait_running(self, *jobs):
        deadline = time.monotonic() + 10
        while any(j.state != "running" for j in jobs) and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual([j.state for j in jobs], ["running"] * len(jobs))

    def closed_hours(self):
        now = time.localtime()
        minute = now.tm_hour * 60 + now.tm_min
        return ((minute + 60) % 1440, (minute + 120) % 1440)  # an hour from now, for an hour

    def test_paused_lane_holds_its_downloads(self):
        self.q = DownloadQueue(workers=1, timeout=10, retries=0, lanes=[Lane("manga", workers=1)]).start()
        self.q.set_lane_paused("manga", True)
        m, d = self.slow("m", "manga"), self.slow("d")
        self.wait_running(d)  # the default lane isn't held up
        time.sleep(0.3)
        self.assertEqual(m.state, "queued")
        self.assertTrue(self.q.lanes_state()[0]["paused"])
        self.q.set_lanes([Lane("manga", workers=2)])  # a kept lane stays paused
        time.sleep(0.3)
        self.assertEqual(m.state, "queued")
        self.q.set_lane_paused("manga", False)
        self.wait_running(m)
        self.assertFalse(self.q.lanes_state()[0]["paused"])
        with self.assertRaisesRegex(ValueError, "no download lane 'books'"):
            self.q.set_lane_paused("books", True)

    def test_removing_a_paused_lane_lets_its_downloads_run(self):
        self.q = DownloadQueue(workers=1, timeout=10, retries=0, lanes=[Lane("manga")]).start()
        self.q.set_lane_paused("manga", True)
        m = self.slow("m", "manga")
        time.sleep(0.3)
        self.assertEqual(m.state, "queued")
        self.q.set_lanes([])
        self.wait_running(m)
        self.q.set_lanes([Lane("manga")])
        self.assertFalse(self.q.lanes_state()[0]["paused"])  # a new lane of the same name starts unpaused

    def fair_order(self, fair):
        self.q = DownloadQueue(workers=2, timeout=10, retries=0, fair=fair)
        self.q.set_paused(True)
        other = self.server.base.replace("127.0.0.1", "localhost")
        a = [self.q.add([self.server.base + "/slow"], self.folder(f"{fair}a{i}"))[0][0] for i in range(3)]
        (b,), _ = self.q.add([other + "/slow"], self.folder(f"{fair}b"))
        self.q.start()
        self.q.set_paused(False)
        return a, b

    def test_fair_takes_turns_between_sites(self):
        a, b = self.fair_order(True)
        self.wait_running(a[0], b)
        time.sleep(0.3)
        self.assertEqual([x.state for x in a[1:]], ["queued", "queued"])

    def test_without_fair_first_queued_first_started(self):
        a, b = self.fair_order(False)
        self.wait_running(a[0], a[1])
        time.sleep(0.3)
        self.assertEqual(b.state, "queued")

    def test_lanes_have_their_own_slots(self):
        self.q = DownloadQueue(workers=1, timeout=10, retries=0, lanes=[Lane("manga", workers=2)]).start()
        default, m1, m2 = self.slow("d"), self.slow("m1", "manga"), self.slow("m2", "manga")
        self.wait_running(default, m1, m2)  # the default lane's one slot doesn't hold up the lane
        m3, d2 = self.slow("m3", "manga"), self.slow("d2")
        time.sleep(0.4)
        self.assertEqual((m3.state, d2.state), ("queued", "queued"))  # each lane is full
        lanes = {x["name"]: x for x in self.q.lanes_state()}
        self.assertEqual((lanes["manga"]["running"], lanes["manga"]["queued"]), (2, 1))
        self.assertEqual((m1.lane, default.lane), ("manga", ""))
        self.q.cancel_all()
        self.q.wait_idle()

    def test_lane_hours_hold_only_that_lane(self):
        lane = Lane("night", workers=1, hours=self.closed_hours())
        self.q = DownloadQueue(workers=1, timeout=10, retries=0, lanes=[lane]).start()
        (night,), _ = self.q.add([self.server.base + "/files/a/x.bin"], self.folder("n"), lane="night")
        (day,), _ = self.q.add([self.server.base + "/files/a/y.bin"], self.folder("d"))
        deadline = time.monotonic() + 10
        while day.state != DONE and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual((day.state, night.state), (DONE, "queued"))
        self.assertTrue(self.q.lanes_state()[0]["waiting"])
        self.assertFalse(self.q.waiting_for_hours())
        self.q.set_lanes([Lane("night", workers=1)])  # no hours of its own: the queue's (none)
        self.q.wait_idle()
        self.assertEqual(night.state, DONE)

    def test_lane_without_hours_follows_the_queue(self):
        self.q = DownloadQueue(workers=1, timeout=10, retries=0, hours=self.closed_hours(), lanes=[Lane("a")])
        self.q.start()
        (job,), _ = self.q.add([self.server.base + "/files/a/x.bin"], self.folder("a"), lane="a")
        time.sleep(0.4)
        self.assertEqual(job.state, "queued")
        self.assertTrue(self.q.lanes_state()[0]["waiting"])

    def test_removed_lane_runs_in_the_default_lane(self):
        self.q = DownloadQueue(workers=1, timeout=10, retries=0, lanes=[Lane("a", hours=self.closed_hours())])
        self.q.start()
        (job,), _ = self.q.add([self.server.base + "/files/a/x.bin"], self.folder("a"), lane="a")
        time.sleep(0.3)
        self.assertEqual(job.state, "queued")
        self.q.set_lanes([])
        self.q.wait_idle()
        self.assertEqual(job.state, DONE)
        with self.assertRaisesRegex(ValueError, "no download lane"):
            self.q.add([self.server.base + "/files/a/z.bin"], self.dir, lane="a")

    def test_set_lanes_adds_threads_and_keeps_budgets(self):
        self.q = DownloadQueue(workers=1, timeout=10, retries=0).start()

        def threads():
            return sum(t.name.startswith("download-") and t.name[9:].isdigit() for t in self.q._threads)

        self.assertEqual(threads(), 1)
        self.q.set_lanes([Lane("a", workers=3, rate=1000), Lane("b", workers=2)])
        self.assertEqual(threads(), 6)
        budget = self.q._lane_limiters["a"]
        self.q.set_lanes([Lane("a", workers=1, rate=5000)])
        self.assertEqual(threads(), 6)  # spare threads wait; the lane's count is what limits it
        self.assertIs(self.q._lane_limiters["a"], budget)
        self.assertEqual(budget.rate, 5000)
        self.assertNotIn("b", self.q._lane_limiters)

    def test_retry_keeps_the_lane(self):
        self.q = DownloadQueue(workers=1, timeout=10, retries=0, lanes=[Lane("a")]).start()
        (job,), _ = self.q.add([self.server.base + "/always500"], self.folder("a"), lane="a")
        self.q.wait_idle()
        self.assertEqual(job.state, FAILED)
        self.q.retry(job.id)
        self.assertEqual(self.q._jobs[job.id].lane, "a")

    def test_lane_checks(self):
        for bad in (
            dict(name=""),
            dict(name="x" * 41),
            dict(name="a\nb"),
            dict(name="a", workers=0),
            dict(name="a", workers=17),
            dict(name="a", workers=True),
            dict(name="a", rate=-1),
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                Lane(**bad)
        q = DownloadQueue()
        with self.assertRaisesRegex(ValueError, "different"):
            q.set_lanes([Lane("A"), Lane("a")])
        self.assertEqual(Lane("  b  ").name, "b")


class RangeTests(unittest.TestCase):
    def test_expand_ranges(self):
        from dlqueue.web import expand_ranges

        self.assertEqual(
            expand_ranges(["http://x/c{1..3}.zip"]), ["http://x/c1.zip", "http://x/c2.zip", "http://x/c3.zip"]
        )
        self.assertEqual(
            expand_ranges(["http://x/v{08..10}.cbz"]),
            ["http://x/v08.cbz", "http://x/v09.cbz", "http://x/v10.cbz"],
        )
        self.assertEqual(expand_ranges(["http://x/a.zip"]), ["http://x/a.zip"])
        for bad in (
            ["http://x/{1..2}/{3..4}"],
            ["http://x/{5..1}"],
            ["http://x/{1..501}"],
            ["http://x/{1..300}", "http://x/{1..300}"],
        ):
            with self.assertRaises(ValueError):
                expand_ranges(bad)


if __name__ == "__main__":
    unittest.main()
