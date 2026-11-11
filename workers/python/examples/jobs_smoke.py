"""End-to-end smoke test for the background job helper.

Run against a temporary daemon:
    PYTHONPATH=client:workers/python NONODB_SOCK=/tmp/nonodb.sock \
      python3 workers/python/examples/jobs_smoke.py
"""

from __future__ import annotations

import os
import time

from nonodb import NonoDB
from nonodb_worker import Job, JobQueue, JobRunner

COLLECTION = os.environ.get("NONODB_JOBS_COLLECTION", "_jobs_smoke")


def main() -> int:
    db = NonoDB()
    try:
        collection = db.collection(COLLECTION)
        collection.delete({}, multi=True)
        queue = JobQueue(collection)
        queue.ensure_indexes()

        add_id = queue.enqueue("add", {"a": 2, "b": 3})
        boom_id = queue.enqueue("boom", max_attempts=1)
        duplicate_id = queue.enqueue("add", {"a": 1, "b": 1}, job_id="fixed-id")
        duplicate_again = queue.enqueue("add", {"a": 9, "b": 9}, job_id="fixed-id")
        assert duplicate_id == duplicate_again == "fixed-id"

        runner = JobRunner(queue, worker_id="smoke")
        runner.register("add", lambda job: {"sum": job.payload["a"] + job.payload["b"]})

        def boom(job: Job):
            raise RuntimeError("expected failure")

        runner.register("boom", boom)

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if not runner.run_once(lease_seconds=5):
                # The failed job is retried once, then marked failed.
                if collection.count({"status": {"$in": ["pending", "running"]}}) == 0:
                    break
                time.sleep(0.05)

        added = collection.find_one({"_id": add_id})
        failed = collection.find_one({"_id": boom_id})
        fixed = collection.find_one({"_id": "fixed-id"})
        assert added and added["status"] == "done" and added["result"]["sum"] == 5
        assert failed and failed["status"] == "failed"
        assert "expected failure" in failed["last_error"]
        assert fixed and fixed["status"] == "done" and fixed["result"]["sum"] == 2
        print("jobs smoke OK")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
