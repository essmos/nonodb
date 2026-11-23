"""End-to-end TTL retention for consumed outbox event collections.

    cargo build -p nonodb-daemon
    python3 scripts/events_ttl_test.py
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))
sys.path.insert(0, str(ROOT / "workers" / "python"))

from nonodb import NonoDB  # noqa: E402
from nonodb_worker import EventConsumer, EventProducer, EventRunner  # noqa: E402


def wait_for_socket(path: Path, proc: subprocess.Popen[bytes], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError("daemon exited before creating its socket")
        time.sleep(0.05)
    raise RuntimeError("timed out waiting for daemon socket")


def wait_until(predicate, timeout: float, message: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError(message)


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.exists():
        print(f"missing daemon binary: {daemon}", file=sys.stderr)
        return 2
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-events-ttl-"))
    sock = tmp / "nonodb.sock"
    log = tmp / "daemon.log"
    env = os.environ.copy()
    env.update(
        {
            "NONODB_DATA": str(tmp / "data"),
            "NONODB_SOCK": str(sock),
            "NONODB_LOG": "warn",
            "NONODB_CACHE_TTL": "0",
            "NONODB_TTL_SWEEP_INTERVAL": "1",
        }
    )
    fh = log.open("wb")
    proc = subprocess.Popen([str(daemon)], env=env, stdout=fh, stderr=subprocess.STDOUT)
    fh.close()
    try:
        wait_for_socket(sock, proc)
        db = NonoDB(str(sock), cache_ttl=0.0)
        try:
            db.create_collection("_events", ttl_seconds=3)
            events = db.collection("_events")
            checkpoints = db.collection("_event_checkpoints")
            producer = EventProducer(events)

            first = producer.emit("ttl.consumed.first", {"i": 1})
            second = producer.emit("ttl.consumed.second", {"i": 2})
            seen: list[str] = []
            consumer = EventConsumer(
                events,
                checkpoints,
                "ttl-consumer",
                overlap_seconds=0,
            )
            runner = EventRunner(consumer)
            for event_type in (
                "ttl.consumed.first",
                "ttl.consumed.second",
                "ttl.consumed.later",
            ):
                runner.register(
                    event_type,
                    lambda event: seen.append(event["_id"]),
                )
            assert runner.run_once(limit=10) == 2, seen
            assert seen == [first, second], seen
            assert consumer.checkpoint() == second, consumer.checkpoint()

            # Consumed events expire without removing the consumer checkpoint.
            wait_until(
                lambda: events.count({"_id": {"$in": [first, second]}}) == 0,
                15,
                "consumed events did not expire",
            )
            assert events.count({}) == 0
            assert consumer.checkpoint() == second, consumer.checkpoint()

            # An unconsumed expired event is not replayed, but it must not block
            # delivery of later events.
            missed = producer.emit("ttl.unconsumed.expired", {"i": 3})
            wait_until(
                lambda: events.count({"_id": missed}) == 0,
                15,
                "unconsumed event did not expire",
            )
            later = producer.emit("ttl.consumed.later", {"i": 4})
            assert runner.run_once(limit=10) == 1, seen
            assert seen == [first, second, later], seen
            assert consumer.checkpoint() == later, consumer.checkpoint()

            # A new consumer instance resumes from the persisted checkpoint.
            restarted = EventConsumer(
                events,
                checkpoints,
                "ttl-consumer",
                overlap_seconds=0,
            )
            restarted_runner = EventRunner(restarted)
            for event_type in ("ttl.consumed.later", "ttl.consumed.newest"):
                restarted_runner.register(event_type, lambda event: None)
            assert restarted_runner.run_once(limit=10) == 0
            assert restarted.checkpoint() == later, restarted.checkpoint()

            # A retained cursor older than the TTL window reports a gap while
            # still returning the remaining retained events.
            newest = producer.emit("ttl.consumed.newest", {"i": 5})
            assert restarted_runner.run_once(limit=10) == 1
            assert restarted.checkpoint() == newest
            batch = events.watch_once(after=first, limit=10, timeout_ms=100)
            assert batch["truncated"] is True, batch
            assert batch["oldest"] == later, batch
            assert [event["_id"] for event in batch["events"]] == [later, newest], batch
        finally:
            db.close()

        print(
            "OK: consumed events expire, checkpoints survive, later events deliver, "
            "expired cursors report truncation"
        )
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
