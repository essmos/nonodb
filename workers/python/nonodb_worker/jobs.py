"""Durable, at-least-once background jobs stored in a normal collection.

The database does not execute job code. A worker process claims a job document,
runs a registered Python handler, and records the result through the normal
NonoDB API. This keeps the daemon and LMDB write path unchanged.

Semantics:
- claims are atomic through ``find_and_modify``;
- a lease token fences stale workers;
- failed jobs retry with exponential backoff until ``max_attempts``;
- handlers must be idempotent because delivery is at-least-once.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

logger = logging.getLogger(__name__)

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_LEASE_SECONDS = 60


def now_ms() -> int:
    """Current UTC time as Unix milliseconds."""
    return int(time.time() * 1000)


def backoff_seconds(attempt: int, *, base: float = 1.0, cap: float = 3600.0) -> float:
    """Exponential retry delay after a failed attempt (1-based)."""
    if attempt <= 0:
        return 0.0
    return min(cap, base * (2 ** (attempt - 1)))


def _job_id(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("job document has no string _id")
    return value


@dataclass(frozen=True)
class Job:
    """A claimed job."""

    id: str
    type: str
    payload: Mapping[str, Any]
    attempts: int
    max_attempts: int
    lease_id: str
    raw: Mapping[str, Any]

    @classmethod
    def from_doc(cls, doc: Mapping[str, Any]) -> "Job":
        return cls(
            id=_job_id(doc.get("_id")),
            type=str(doc.get("type") or ""),
            payload=doc.get("payload") if isinstance(doc.get("payload"), Mapping) else {},
            attempts=int(doc.get("attempts") or 0),
            max_attempts=int(doc.get("max_attempts") or DEFAULT_MAX_ATTEMPTS),
            lease_id=str(doc.get("lease_id") or ""),
            raw=doc,
        )


class JobQueue:
    """Job operations over a NonoDB collection."""

    def __init__(self, collection: Any, *, clock: Callable[[], int] = now_ms):
        self.collection = collection
        self._clock = clock

    def ensure_indexes(self) -> list[str]:
        """Create the indexes used by claim and expired-lease recovery.

        This is optional and normally run once by an administrator. Creating an
        index rebuilds it, so do not call it on every worker start in large
        deployments.
        """
        return self.collection.create_indexes(
            [
                {"fields": ["status", "due_at"]},
                {"fields": ["status", "lease_until"]},
            ]
        )

    def enqueue(
        self,
        job_type: str,
        payload: Optional[Mapping[str, Any]] = None,
        *,
        job_id: Optional[str] = None,
        delay_seconds: float = 0.0,
        due_at: Optional[int] = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> str:
        """Insert a pending job and return its ``_id``.

        Passing ``job_id`` makes enqueue idempotent: a duplicate-key error for
        that id is treated as an already-enqueued job.
        """
        if not job_type:
            raise ValueError("job_type must not be empty")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        now = self._clock()
        due = due_at if due_at is not None else now + int(delay_seconds * 1000)
        doc: dict[str, Any] = {
            "type": job_type,
            "status": "pending",
            "payload": dict(payload or {}),
            "attempts": 0,
            "max_attempts": int(max_attempts),
            "due_at": int(due),
            "created_at": now,
            "updated_at": now,
        }
        if job_id is not None:
            doc["_id"] = job_id
        try:
            return self.collection.insert(doc)
        except Exception as exc:
            if job_id is not None and getattr(exc, "code", None) == "duplicate_key":
                return job_id
            raise

    def claim(
        self,
        *,
        worker_id: str,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
    ) -> Optional[Job]:
        """Atomically claim one due pending job."""
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be at least 1")
        now = self._clock()
        lease_id = uuid.uuid4().hex
        doc = self.collection.find_and_modify(
            {
                "status": "pending",
                "due_at": {"$lte": now},
            },
            {
                "$set": {
                    "status": "running",
                    "lease_id": lease_id,
                    "lease_until": now + lease_seconds * 1000,
                    "worker_id": worker_id,
                    "started_at": now,
                    "updated_at": now,
                },
                "$inc": {"attempts": 1},
            },
            new=True,
            sort=[("due_at", 1)],
        )
        if doc is None:
            return None
        return Job.from_doc(doc)

    def complete(self, job: Job, result: Any = None) -> bool:
        """Mark a job done, but only if this worker still owns the lease."""
        now = self._clock()
        update: dict[str, Any] = {
            "$set": {
                "status": "done",
                "updated_at": now,
                "finished_at": now,
                "result": result,
            },
            "$unset": {
                "lease_id": "",
                "lease_until": "",
                "worker_id": "",
            },
        }
        return (
            self.collection.update(
                {
                    "_id": job.id,
                    "status": "running",
                    "lease_id": job.lease_id,
                },
                update,
            )
            == 1
        )

    def fail(
        self,
        job: Job,
        error: str,
        *,
        retry_delay: Optional[float] = None,
        retry: bool = True,
    ) -> bool:
        """Record a failure and either retry or mark the job failed."""
        now = self._clock()
        error = error[:4000]
        if not retry or job.attempts >= job.max_attempts:
            status = "failed"
            due_at = now
        else:
            status = "pending"
            delay = (
                backoff_seconds(job.attempts)
                if retry_delay is None
                else max(0.0, retry_delay)
            )
            due_at = now + int(delay * 1000)
        update = {
            "$set": {
                "status": status,
                "last_error": error,
                "due_at": due_at,
                "updated_at": now,
            },
            "$unset": {
                "lease_id": "",
                "lease_until": "",
                "worker_id": "",
            },
        }
        if status == "failed":
            update["$set"]["failed_at"] = now
        return (
            self.collection.update(
                {
                    "_id": job.id,
                    "status": "running",
                    "lease_id": job.lease_id,
                },
                update,
            )
            == 1
        )

    def requeue_expired(self, *, limit: int = 100) -> int:
        """Recover jobs whose worker lease expired.

        A stale worker can still finish locally, but its fenced update will not
        overwrite the recovered job.
        """
        now = self._clock()
        expired = self.collection.find(
            {"status": "running", "lease_until": {"$lt": now}},
            limit=limit,
        )
        recovered = 0
        for doc in expired:
            attempts = int(doc.get("attempts") or 0)
            max_attempts = int(doc.get("max_attempts") or DEFAULT_MAX_ATTEMPTS)
            status = "failed" if attempts >= max_attempts else "pending"
            update: dict[str, Any] = {
                "$set": {
                    "status": status,
                    "due_at": now,
                    "last_error": "lease expired",
                    "updated_at": now,
                },
                "$unset": {
                    "lease_id": "",
                    "lease_until": "",
                    "worker_id": "",
                },
            }
            if status == "failed":
                update["$set"]["failed_at"] = now
            recovered += self.collection.update(
                {
                    "_id": doc["_id"],
                    "status": "running",
                    "lease_until": {"$lt": now},
                },
                update,
            )
        return recovered


class JobRunner:
    """Dispatch claimed jobs to registered Python handlers."""

    def __init__(
        self,
        queue: JobQueue,
        *,
        worker_id: Optional[str] = None,
    ):
        self.queue = queue
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:12]}"
        self.handlers: dict[str, Callable[[Job], Any]] = {}
        self._stop = threading.Event()

    def register(self, job_type: str, handler: Callable[[Job], Any]) -> None:
        if not job_type:
            raise ValueError("job_type must not be empty")
        self.handlers[job_type] = handler

    def stop(self) -> None:
        self._stop.set()

    def run_once(self, *, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> bool:
        """Claim and execute at most one job. Returns False when idle."""
        job = self.queue.claim(worker_id=self.worker_id, lease_seconds=lease_seconds)
        if job is None:
            return False

        handler = self.handlers.get(job.type)
        if handler is None:
            logger.error("unknown job type %r for job %s", job.type, job.id)
            self.queue.fail(
                job,
                f"unknown job type: {job.type}",
                retry_delay=0,
                retry=False,
            )
            return True

        try:
            result = handler(job)
        except Exception as exc:  # noqa: BLE001 - handlers are user code
            message = f"{type(exc).__name__}: {exc}"
            logger.exception("job %s (%s) failed", job.id, job.type)
            if not self.queue.fail(job, message):
                logger.warning("job %s lease was lost before failure was recorded", job.id)
            return True

        if not self.queue.complete(job, result):
            logger.warning("job %s lease was lost before completion was recorded", job.id)
        return True

    def run_forever(
        self,
        *,
        poll_interval: float = 1.0,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        recover_interval: float = 30.0,
    ) -> None:
        """Run until ``stop()`` is called, recovering expired leases periodically."""
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        last_recover = 0.0
        while not self._stop.is_set():
            if recover_interval > 0:
                now = time.monotonic()
                if now - last_recover >= recover_interval:
                    recovered = self.queue.requeue_expired()
                    if recovered:
                        logger.warning("requeued %d expired job(s)", recovered)
                    last_recover = now
            if self.run_once(lease_seconds=lease_seconds):
                continue
            self._stop.wait(poll_interval)
