"""In-memory job queue base: pending list, workers, pause, cancel, retry and clear.

cbz-webp has the same ``BaseQueue`` in ``cbzwebp/jobqueue.py``; keep the two in step.
"""

from __future__ import annotations

import itertools
import threading
import time
import traceback
from collections.abc import Callable
from collections import deque


QUEUED, RUNNING, DONE, SKIPPED, FAILED, CANCELLED = (
    "queued",
    "running",
    "done",
    "skipped",
    "failed",
    "cancelled",
)
ACTIVE = {QUEUED, RUNNING}
FINISHED = {DONE, SKIPPED, FAILED, CANCELLED}


class BaseQueue:
    """Shared queue machinery. Subclasses implement ``_run`` and may override ``_busy_key``.

    Jobs need ``id``, ``state``, ``message``, ``started_at``, ``finished_at``, a ``cancel``
    event, ``to_dict()`` and ``fresh()``.
    """

    thread_prefix = "worker"

    def __init__(self, workers: int = 1, on_change: Callable | None = None):
        self.workers = max(1, workers)
        self.on_change = on_change
        self._jobs: dict = {}
        self._pending: deque[int] = deque()
        self._ids = itertools.count(1)
        self._cond = threading.Condition()
        self._paused = False
        self._stopping = False
        self._threads: list[threading.Thread] = []

    # ------------------------------------------------------------ lifecycle
    def start(self):
        for n in range(self.workers):
            t = threading.Thread(target=self._worker, name=f"{self.thread_prefix}-{n}", daemon=True)
            t.start()
            self._threads.append(t)
        return self

    def shutdown(self, cancel_running: bool = True) -> None:
        with self._cond:
            self._stopping = True
            for jid in list(self._pending):
                self._finish(self._jobs[jid], CANCELLED, "queue stopped")
            self._pending.clear()
            if cancel_running:
                for j in self._jobs.values():
                    if j.state == RUNNING:
                        j.cancel.set()
            self._cond.notify_all()
        for t in self._threads:
            t.join()
        self._after_shutdown()

    def _after_shutdown(self) -> None:
        pass

    # ------------------------------------------------------------ commands
    def _enqueue(self, jobs: list) -> None:
        """Register new jobs and wake the workers. Call with the lock held."""
        for job in jobs:
            self._jobs[job.id] = job
            self._pending.append(job.id)
        self._cond.notify_all()

    def _new_id(self) -> int:
        return next(self._ids)

    def cancel(self, job_id: int) -> bool:
        with self._cond:
            job = self._jobs.get(job_id)
            if not job or job.state not in ACTIVE:
                return False
            if job.state == QUEUED:
                self._pending.remove(job_id)
                self._finish(job, CANCELLED, "cancelled before start")
            else:
                job.cancel.set()
                job.message = "cancelling…"
        self._emit(job)
        return True

    def cancel_all(self) -> int:
        """Cancel every queued and running job in one locked pass; emit once per job after."""
        with self._cond:
            changed = []
            for job in self._jobs.values():
                if job.state == QUEUED:
                    self._finish(job, CANCELLED, "cancelled before start")
                elif job.state == RUNNING:
                    job.cancel.set()
                    job.message = "cancelling…"
                else:
                    continue
                changed.append(job)
            self._pending.clear()
        for job in changed:
            self._emit(job)
        return len(changed)

    def retry(self, job_id: int) -> bool:
        with self._cond:
            job = self._jobs.get(job_id)
            if not job or job.state not in (FAILED, CANCELLED):
                return False
            key = self._dedup_key(job)
            if any(self._dedup_key(j) == key and j.state in ACTIVE for j in self._jobs.values()):
                return False
            fresh = job.fresh()
            self._enqueue([fresh])
        self._emit(fresh)
        return True

    def clear_finished(self) -> int:
        with self._cond:
            gone = [i for i, j in self._jobs.items() if j.state in FINISHED]
            for i in gone:
                del self._jobs[i]
        return len(gone)

    def set_paused(self, paused: bool) -> None:
        with self._cond:
            self._paused = paused
            self._cond.notify_all()

    # ------------------------------------------------------------ queries
    @property
    def paused(self) -> bool:
        return self._paused

    def snapshot(self) -> list[dict]:
        with self._cond:
            return [j.to_dict() for j in self._jobs.values()]

    def jobs(self) -> list:
        with self._cond:
            return list(self._jobs.values())

    def wait_idle(self, poll: float = 0.2) -> None:
        with self._cond:
            while self._pending or any(j.state == RUNNING for j in self._jobs.values()):
                self._cond.wait(poll)

    # ------------------------------------------------------------ internals
    def _emit(self, job) -> None:
        if self.on_change:
            try:
                self.on_change(job)
            except Exception:  # noqa: BLE001 - a broken listener must not kill a worker
                traceback.print_exc()

    def _finish(self, job, state: str, message: str) -> None:
        job.state, job.message, job.finished_at = state, message, time.time()

    def _dedup_key(self, job):
        """Jobs with equal keys are never queued or running twice."""
        return job.id

    def _busy_key(self, job):
        """Jobs with equal non-None keys never run at the same time."""
        return None

    def _next_runnable(self):
        """First pending job whose busy key no running job holds. Call with the lock held."""
        busy = {self._busy_key(j) for j in self._jobs.values() if j.state == RUNNING} - {None}
        for jid in self._pending:
            if self._busy_key(self._jobs[jid]) not in busy:
                self._pending.remove(jid)
                return self._jobs[jid]
        return None

    def _worker(self) -> None:
        while True:
            with self._cond:
                job = None
                while not self._stopping:
                    if not self._paused and (job := self._next_runnable()):
                        break
                    self._cond.wait()
                if self._stopping:
                    return
                job.state, job.started_at, job.message = RUNNING, time.time(), ""
            self._emit(job)
            self._run(job)
            with self._cond:
                self._cond.notify_all()
            self._emit(job)

    def _run(self, job) -> None:
        raise NotImplementedError
