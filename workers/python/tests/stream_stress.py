"""Real-daemon stream stress test.

Starts a temporary daemon, runs concurrent outbox producers and independent
NDJSON stream consumers, then verifies every produced event was delivered
exactly once to every consumer.

    PRODUCERS=16 PER=1000 CONSUMERS=16 \
        python3 workers/python/tests/stream_stress.py

This file is intentionally not named ``test_*.py``; the normal unit-test
discovery must not require a built daemon binary.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "client"))
sys.path.insert(0, str(ROOT / "workers" / "python"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from events_integration import Daemon  # noqa: E402
from nonodb_worker import EventProducer  # noqa: E402


def main() -> int:
    producers = int(os.environ.get("PRODUCERS", "8"))
    per_producer = int(os.environ.get("PER", "500"))
    consumers = int(os.environ.get("CONSUMERS", "8"))
    collection = "_stream_stress_events"
    daemon = Daemon()
    try:
        db = daemon.client()
        try:
            db.collection(collection).delete({}, multi=True)
        finally:
            db.close()

        produced: list[str] = []
        produced_lock = threading.Lock()
        producer_errors: list[BaseException] = []
        consumer_errors: list[BaseException] = []
        consumed: list[list[str]] = [[] for _ in range(consumers)]
        expected_ids: set[str] | None = None

        def produce(thread_id: int) -> None:
            client = daemon.client()
            try:
                producer = EventProducer(client.collection(collection))
                ids = [
                    producer.emit("stress", {"thread": thread_id, "i": index})
                    for index in range(per_producer)
                ]
                with produced_lock:
                    produced.extend(ids)
            except BaseException as exc:  # noqa: BLE001 - report from thread
                producer_errors.append(exc)
            finally:
                client.close()

        def consume(index: int) -> None:
            client = daemon.client()
            try:
                stream = client.collection(collection).watch_stream(
                    after=None,
                    limit=500,
                    timeout_ms=70_000,
                    heartbeat_ms=1_000,
                    retry=False,
                )
                for event in stream:
                    event_id = event.get("_id")
                    if event_id:
                        consumed[index].append(event_id)
                    if expected_ids is not None and expected_ids <= set(consumed[index]):
                        stream.close()
                        break
            except BaseException as exc:  # noqa: BLE001 - report from thread
                consumer_errors.append(exc)
            finally:
                client.close()

        consumer_threads = [
            threading.Thread(target=consume, args=(index,), name=f"stream-{index}")
            for index in range(consumers)
        ]
        for thread in consumer_threads:
            thread.start()
        time.sleep(0.2)

        started = time.monotonic()
        producer_threads = [
            threading.Thread(target=produce, args=(index,), name=f"producer-{index}")
            for index in range(producers)
        ]
        for thread in producer_threads:
            thread.start()
        for thread in producer_threads:
            thread.join()
        if producer_errors:
            raise producer_errors[0]

        expected_ids = set(produced)
        assert len(expected_ids) == producers * per_producer
        # Wake consumers that reached the end before expected_ids was known.
        db = daemon.client()
        try:
            EventProducer(db.collection(collection)).emit(
                "stress.done", {"produced": len(expected_ids)}
            )
        finally:
            db.close()

        for thread in consumer_threads:
            thread.join(timeout=80)
        elapsed = time.monotonic() - started
        if any(thread.is_alive() for thread in consumer_threads):
            raise AssertionError("stream consumers did not finish")
        if consumer_errors:
            raise consumer_errors[0]

        print(
            f"produced={len(expected_ids)} producers={producers} "
            f"consumers={consumers} elapsed={elapsed:.2f}s"
        )
        failed = False
        for index, ids in enumerate(consumed):
            unique = set(ids)
            missing = expected_ids - unique
            duplicates = len(ids) - len(unique)
            print(
                f"consumer {index}: got={len(ids)} unique={len(unique)} "
                f"missing={len(missing)} duplicate={duplicates}"
            )
            if missing or duplicates:
                failed = True
        if failed:
            return 1

        time.sleep(0.5)
        db = daemon.client()
        try:
            metrics = db._http.get(db.base_url + "/metrics").json()
        finally:
            db.close()
        print(
            "metrics: "
            f"watch_streams_total={metrics['watch_streams_total']} "
            f"watch_active={metrics['watch_active']} "
            f"watch_events_total={metrics['watch_events_total']}"
        )
        print("stream stress OK: 0 missing, 0 duplicate deliveries")
        return 0
    finally:
        daemon.stop()


if __name__ == "__main__":
    raise SystemExit(main())
