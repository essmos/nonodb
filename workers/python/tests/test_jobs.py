"""Unit tests for the background job helper."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any, Mapping, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nonodb_worker import Job, JobRunner, backoff_seconds  # noqa: E402


class FakeQueue:
    def __init__(self, job: Optional[Job]):
        self.job = job
        self.completed: list[tuple[Job, Any]] = []
        self.failed: list[tuple[Job, str]] = []

    def claim(self, *, worker_id: str, lease_seconds: int = 60) -> Optional[Job]:
        job, self.job = self.job, None
        return job

    def complete(self, job: Job, result: Any = None) -> bool:
        self.completed.append((job, result))
        return True

    def fail(
        self,
        job: Job,
        error: str,
        *,
        retry_delay: Optional[float] = None,
        retry: bool = True,
    ) -> bool:
        self.failed.append((job, error))
        return True

    def requeue_expired(self, *, limit: int = 100) -> int:
        return 0


def job(job_type: str, payload: Mapping[str, Any] | None = None) -> Job:
    return Job(
        id="j1",
        type=job_type,
        payload=payload or {},
        attempts=1,
        max_attempts=3,
        lease_id="lease",
        raw={},
    )


class JobRunnerTests(unittest.TestCase):
    def test_backoff_is_exponential_and_capped(self) -> None:
        self.assertEqual(backoff_seconds(0), 0.0)
        self.assertEqual(backoff_seconds(1), 1.0)
        self.assertEqual(backoff_seconds(3), 4.0)
        self.assertEqual(backoff_seconds(100), 3600.0)

    def test_success_is_completed(self) -> None:
        queue = FakeQueue(job("add", {"a": 2, "b": 3}))
        runner = JobRunner(queue, worker_id="test")
        runner.register("add", lambda j: {"sum": j.payload["a"] + j.payload["b"]})
        self.assertTrue(runner.run_once())
        self.assertEqual(len(queue.completed), 1)
        self.assertEqual(queue.completed[0][1], {"sum": 5})
        self.assertFalse(runner.run_once())

    def test_handler_error_is_failed(self) -> None:
        queue = FakeQueue(job("boom"))
        runner = JobRunner(queue, worker_id="test")

        def boom(_job):
            raise RuntimeError("boom")

        runner.register("boom", boom)
        self.assertTrue(runner.run_once())
        self.assertEqual(len(queue.failed), 1)
        self.assertIn("RuntimeError: boom", queue.failed[0][1])

    def test_unknown_type_fails(self) -> None:
        queue = FakeQueue(job("missing"))
        runner = JobRunner(queue, worker_id="test")
        self.assertTrue(runner.run_once())
        self.assertEqual(len(queue.failed), 1)
        self.assertIn("unknown job type", queue.failed[0][1])


if __name__ == "__main__":
    unittest.main()
