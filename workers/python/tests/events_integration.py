"""Real-daemon integration tests for outbox events.

This is intentionally not named ``test_*.py`` so the fast unit-test discovery
does not require a daemon binary. Run it explicitly:

    python3 workers/python/tests/events_integration.py

It starts ``target/debug/nonodb-daemon`` with a temporary data directory, then
exercises atomic writes, checkpoints, retries, late out-of-order events,
concurrent producers, and the event worker CLI.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "client"))
sys.path.insert(0, str(ROOT / "workers" / "python"))

from nonodb import NonoDB, NonoDBError, StreamTruncatedError  # noqa: E402
from nonodb_worker import (  # noqa: E402
    EventConsumer,
    EventProducer,
    EventRunner,
    new_event_id,
    now_ms,
)


class Daemon:
    def __init__(self) -> None:
        self.temp = Path(tempfile.mkdtemp(prefix="nonodb-events-it-"))
        self.socket = self.temp / "nonodb.sock"
        self.log = self.temp / "daemon.log"
        binary = Path(os.environ.get("NONODB_BIN", str(ROOT / "target/debug/nonodb-daemon")))
        if not binary.exists():
            shutil.rmtree(self.temp, ignore_errors=True)
            raise SystemExit(
                f"daemon binary not found: {binary}\n"
                "build it with: cargo build -p nonodb-daemon"
            )
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(self.temp / "data"),
            NONODB_SOCK=str(self.socket),
            NONODB_CACHE_TTL="0",
        )
        self._log_file = self.log.open("wb")
        self.proc = subprocess.Popen(
            [str(binary)],
            env=env,
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.socket.exists():
                return
            if self.proc.poll() is not None:
                break
            time.sleep(0.05)
        self.stop()
        raise SystemExit("daemon did not start:\n" + self.log.read_text(errors="replace"))

    def stop(self) -> None:
        if getattr(self, "proc", None) is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        if getattr(self, "_log_file", None) is not None:
            self._log_file.close()
        shutil.rmtree(self.temp, ignore_errors=True)

    def client(self) -> NonoDB:
        return NonoDB(socket_path=str(self.socket))

    def env(self, module_path: Path | None = None) -> dict[str, str]:
        path = [str(ROOT / "client"), str(ROOT / "workers" / "python")]
        if module_path is not None:
            path.append(str(module_path))
        env = os.environ.copy()
        env.update(NONODB_SOCK=str(self.socket), PYTHONPATH=os.pathsep.join(path))
        return env


def clean(*collections: Any) -> None:
    for collection in collections:
        collection.delete({}, multi=True)


def scenario_atomicity(daemon: Daemon) -> None:
    db = daemon.client()
    try:
        events = db.collection("_it_atomic_events")
        data = db.collection("_it_atomic_data")
        clean(events, data)
        data.insert({"_id": "dup", "value": 1})
        producer = EventProducer(events)
        bad_id = new_event_id()
        try:
            producer.emit(
                "atomic.bad",
                {"n": 1},
                event_id=bad_id,
                data_ops=[
                    {
                        "op": "insert",
                        "coll": "_it_atomic_data",
                        "doc": {"_id": "dup", "value": 2},
                    }
                ],
            )
        except NonoDBError:
            pass
        else:
            raise AssertionError("duplicate data write should fail the whole batch")
        assert events.find_one({"_id": bad_id}) is None
        assert data.find_one({"_id": "dup"})["value"] == 1

        good_id = producer.emit(
            "atomic.good",
            {"n": 2},
            data_ops=[
                {
                    "op": "insert",
                    "coll": "_it_atomic_data",
                    "doc": {"_id": "ok", "value": 2},
                }
            ],
        )
        assert events.find_one({"_id": good_id})["type"] == "atomic.good"
        assert data.find_one({"_id": "ok"})["value"] == 2
        print("  atomic data + event rollback/commit OK")
    finally:
        db.close()


def scenario_checkpoint_and_restart(daemon: Daemon) -> None:
    db = daemon.client()
    try:
        events = db.collection("_it_checkpoint_events")
        data = db.collection("_it_checkpoint_data")
        checkpoints = db.collection("_it_checkpoint_cp")
        clean(events, data, checkpoints)
        producer = EventProducer(events)
        ids = [
            producer.emit(
                "checkpoint",
                {"i": i},
                data_ops=[
                    {
                        "op": "insert",
                        "coll": "_it_checkpoint_data",
                        "doc": {"_id": f"d{i}", "i": i},
                    }
                ],
            )
            for i in range(5)
        ]
        seen: list[str] = []
        consumer = EventConsumer(events, checkpoints, "checkpoint", overlap_seconds=0)
        runner = EventRunner(consumer)
        runner.register("checkpoint", lambda event: seen.append(event["_id"]))
        while runner.run_once(limit=2):
            pass
        assert seen == sorted(ids), seen
        assert consumer.checkpoint() == max(ids)
        assert data.count({}) == 5

        restarted = EventConsumer(events, checkpoints, "checkpoint", overlap_seconds=0)
        restarted_runner = EventRunner(restarted)
        restarted_seen: list[str] = []
        restarted_runner.register(
            "checkpoint", lambda event: restarted_seen.append(event["_id"])
        )
        assert restarted_runner.run_once(limit=10) == 0
        assert restarted_seen == []
        print("  checkpoint persistence and restart OK")
    finally:
        db.close()


def scenario_failure_retry(daemon: Daemon) -> None:
    db = daemon.client()
    try:
        events = db.collection("_it_retry_events")
        checkpoints = db.collection("_it_retry_cp")
        clean(events, checkpoints)
        producer = EventProducer(events)
        ids = [
            producer.emit("ok", {"i": 1}),
            producer.emit("retry", {"i": 2}),
            producer.emit("ok", {"i": 3}),
        ]
        attempts = 0
        seen: list[str] = []

        def retry(event: dict[str, Any]) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("expected first failure")
            seen.append(event["_id"])

        consumer = EventConsumer(events, checkpoints, "retry", overlap_seconds=0)
        runner = EventRunner(consumer)
        runner.register("ok", lambda event: seen.append(event["_id"]))
        runner.register("retry", retry)
        assert runner.run_once(limit=10) == 1
        assert consumer.checkpoint() == ids[0]
        assert seen == [ids[0]]
        while runner.run_once(limit=10):
            pass
        assert seen == ids, seen
        assert attempts == 2
        assert consumer.checkpoint() == ids[2]
        print("  handler failure, checkpoint boundary, and retry OK")
    finally:
        db.close()


def scenario_late_out_of_order_event(daemon: Daemon) -> None:
    db = daemon.client()
    try:
        events = db.collection("_it_late_events")
        checkpoints = db.collection("_it_late_cp")
        clean(events, checkpoints)
        producer = EventProducer(events)
        now = now_ms()
        low_id = new_event_id(now - 1000)
        high_id = new_event_id(now)
        assert low_id < high_id
        producer.emit("late", {"id": "high"}, event_id=high_id)

        seen: list[str] = []
        consumer = EventConsumer(
            events,
            checkpoints,
            "late",
            overlap_seconds=2.0,
        )
        runner = EventRunner(consumer)
        runner.register("late", lambda event: seen.append(event["_id"]))
        assert runner.run_once(limit=10) == 1
        assert seen == [high_id]

        # Simulate a lower id whose commit lands after the consumer already
        # advanced to a newer id. The overlap rescan must recover it.
        producer.emit("late", {"id": "low"}, event_id=low_id)
        time.sleep(2.2)
        assert runner.run_once(limit=10) == 1
        assert seen == [high_id, low_id], seen
        assert consumer.checkpoint() == high_id
        assert runner.run_once(limit=10) == 0
        print("  late out-of-order event recovered by overlap scan OK")
    finally:
        db.close()


def scenario_independent_consumers(daemon: Daemon) -> None:
    db = daemon.client()
    try:
        events = db.collection("_it_independent_events")
        checkpoints = db.collection("_it_independent_cp")
        clean(events, checkpoints)
        producer = EventProducer(events)
        ids = [producer.emit("fanout", {"i": i}) for i in range(3)]
        seen: dict[str, list[str]] = {"a": [], "b": []}
        for name in ("a", "b"):
            consumer = EventConsumer(events, checkpoints, name, overlap_seconds=0)
            runner = EventRunner(consumer)
            runner.register(
                "fanout",
                lambda event, name=name: seen[name].append(event["_id"]),
            )
            while runner.run_once(limit=10):
                pass
        assert seen["a"] == sorted(ids), seen["a"]
        assert seen["b"] == sorted(ids), seen["b"]
        print("  independent consumer checkpoints OK")
    finally:
        db.close()


def scenario_concurrent_producers(daemon: Daemon) -> None:
    events_name = "_it_concurrent_events"
    data_name = "_it_concurrent_data"
    cp_name = "_it_concurrent_cp"
    db = daemon.client()
    try:
        clean(
            db.collection(events_name),
            db.collection(data_name),
            db.collection(cp_name),
        )
    finally:
        db.close()

    produced: list[str] = []
    seen: list[str] = []
    lock = threading.Lock()
    stop = threading.Event()
    errors: list[BaseException] = []
    threads = 8
    per_thread = 100

    def produce(thread_id: int) -> None:
        db = daemon.client()
        try:
            producer = EventProducer(db.collection(events_name))
            local = [
                producer.emit(
                    "concurrent",
                    {"thread": thread_id, "i": i},
                    doc_id=f"{thread_id}:{i}",
                    data_ops=[
                        {
                            "op": "insert",
                            "coll": data_name,
                            "doc": {
                                "_id": f"{thread_id}:{i}",
                                "thread": thread_id,
                                "i": i,
                            },
                        }
                    ],
                )
                for i in range(per_thread)
            ]
            with lock:
                produced.extend(local)
        finally:
            db.close()

    def consume() -> None:
        try:
            db = daemon.client()
            try:
                consumer = EventConsumer(
                    db.collection(events_name),
                    db.collection(cp_name),
                    "concurrent",
                    overlap_seconds=1.0,
                )
                runner = EventRunner(consumer)

                def handler(event: dict[str, Any]) -> None:
                    with lock:
                        seen.append(event["_id"])

                runner.register("concurrent", handler)
                while not stop.is_set():
                    runner.run_once(limit=200)
                    time.sleep(0.001)
                for _ in range(50):
                    if runner.run_once(limit=1000) == 0:
                        break
                    time.sleep(0.05)
            finally:
                db.close()
        except BaseException as exc:  # noqa: BLE001 - report from the thread
            errors.append(exc)

    consumer_thread = threading.Thread(target=consume, name="event-consumer")
    consumer_thread.start()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(produce, range(threads)))

    # Let the one-second safety window age out so late lower ids are rescanned.
    time.sleep(2.0)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        with lock:
            if len(set(seen)) >= len(produced):
                break
        time.sleep(0.05)
    stop.set()
    consumer_thread.join(timeout=20)
    if consumer_thread.is_alive():
        raise AssertionError("consumer thread did not stop")
    if errors:
        raise errors[0]

    with lock:
        seen_set = set(seen)
        duplicate_count = len(seen) - len(seen_set)
    missing = set(produced) - seen_set
    assert not missing, f"missing {len(missing)} events: {list(missing)[:5]}"
    assert len(set(produced)) == threads * per_thread
    db = daemon.client()
    try:
        assert db.collection(data_name).count({}) == threads * per_thread
    finally:
        db.close()
    print(
        "  concurrent producers: "
        f"{len(produced)} events, 0 missing, {duplicate_count} duplicate deliveries OK"
    )


def scenario_watch_stream(daemon: Daemon) -> None:
    db = daemon.client()
    try:
        events = db.collection("_it_watch_events")
        clean(events)
        producer = EventProducer(events)
        first = producer.emit("watch", {"i": 1})
        second = producer.emit("watch", {"i": 2})

        batch = events.watch_once(limit=10, timeout_ms=100)
        assert [event["_id"] for event in batch["events"]] == [first, second]
        assert batch["next"] == second
        assert batch["timed_out"] is False
        assert batch["has_more"] is False

        started = time.monotonic()
        empty = events.watch_once(batch["next"], timeout_ms=200)
        assert empty["events"] == []
        assert empty["timed_out"] is True
        assert time.monotonic() - started >= 0.15

        thread_errors: list[BaseException] = []

        def emit_later() -> None:
            try:
                time.sleep(0.2)
                other = daemon.client()
                try:
                    EventProducer(other.collection("_it_watch_events")).emit(
                        "watch", {"i": 3}
                    )
                finally:
                    other.close()
            except BaseException as exc:  # noqa: BLE001 - report from the thread
                thread_errors.append(exc)

        thread = threading.Thread(target=emit_later)
        thread.start()
        started = time.monotonic()
        live = events.watch_once(second, timeout_ms=2000)
        thread.join(timeout=5)
        assert not thread_errors, thread_errors
        assert len(live["events"]) == 1
        assert live["events"][0]["payload"] == {"i": 3}
        assert time.monotonic() - started >= 0.15
        third = live["next"]

        producer.emit("watch.other", {"i": 4})
        fourth = producer.emit("watch.target", {"i": 5})
        filtered = events.watch_once(
            third,
            limit=10,
            timeout_ms=100,
            types=["watch.target"],
        )
        assert [event["_id"] for event in filtered["events"]] == [fourth]

        page_ids = [
            producer.emit("watch.page", {"i": i}) for i in range(4)
        ]
        page = events.watch_once(fourth, limit=2, timeout_ms=100)
        assert [event["_id"] for event in page["events"]] == page_ids[:2]
        assert page["has_more"] is True
        page2 = events.watch_once(page["next"], limit=2, timeout_ms=100)
        assert [event["_id"] for event in page2["events"]] == page_ids[2:]
        assert page2["has_more"] is False

        generated_id = producer.emit("watch.page", {"i": 99})
        generator = events.watch(
            page2["next"],
            limit=2,
            timeout_ms=100,
            types=["watch.page"],
        )
        assert next(generator)["_id"] == generated_id
        generator.close()

        filtered_id = producer.emit("watch.filtered", {"i": 42}, doc_id="f42")
        filtered = events.watch_once(
            fourth,
            limit=10,
            timeout_ms=100,
            filter={"payload.i": 42},
            projection={"payload": 1, "_id": 0},
        )
        assert [event["_id"] for event in filtered["events"]] == [filtered_id]
        assert filtered["events"][0]["payload"] == {"i": 42}
        assert "_id" in filtered["events"][0]  # always retained for resume

        truncated_batch = events.watch_once("0", limit=1, timeout_ms=100)
        assert truncated_batch["truncated"] is True
        assert truncated_batch["oldest"] == first
        truncated_generator = events.watch("0", limit=1, timeout_ms=100)
        try:
            next(truncated_generator)
        except StreamTruncatedError as exc:
            assert exc.oldest == first
        else:
            raise AssertionError("old resume cursor should report truncation")

        stream_thread_errors: list[BaseException] = []

        def emit_stream_event() -> None:
            try:
                time.sleep(0.2)
                other = daemon.client()
                try:
                    EventProducer(other.collection("_it_watch_events")).emit(
                        "watch.stream", {"i": 7}
                    )
                finally:
                    other.close()
            except BaseException as exc:  # noqa: BLE001 - report from the thread
                stream_thread_errors.append(exc)

        stream_thread = threading.Thread(target=emit_stream_event)
        stream_thread.start()
        streamed = events.watch_stream(
            first,
            timeout_ms=2000,
            heartbeat_ms=100,
            filter={"payload.i": 7},
            projection={"payload": 1},
            retry=False,
        )
        stream_event = next(streamed)
        streamed.close()
        stream_thread.join(timeout=5)
        assert not stream_thread_errors, stream_thread_errors
        assert stream_event["payload"] == {"i": 7}
        assert stream_event["_id"] > first

        sse_thread_errors: list[BaseException] = []

        def emit_sse_event() -> None:
            try:
                time.sleep(0.2)
                other = daemon.client()
                try:
                    EventProducer(other.collection("_it_watch_events")).emit(
                        "watch.sse", {"i": 8}
                    )
                finally:
                    other.close()
            except BaseException as exc:  # noqa: BLE001 - report from the thread
                sse_thread_errors.append(exc)

        sse_thread = threading.Thread(target=emit_sse_event)
        sse_thread.start()
        sse_event: dict[str, Any] | None = None
        with db._http.stream(
            "POST",
            db.base_url + "/v1/watch",
            content=json.dumps(
                {
                    "op": "watch",
                    "coll": "_it_watch_events",
                    "after": first,
                    "timeout_ms": 2000,
                    "heartbeat_ms": 100,
                    "filter": {"payload.i": 8},
                    "projection": {"payload": 1},
                },
                separators=(",", ":"),
            ).encode(),
            headers={
                "content-type": "application/json",
                "accept": "text/event-stream",
            },
            timeout=5,
        ) as resp:
            assert resp.status_code == 200
            assert "text/event-stream" in resp.headers["content-type"]
            for line in resp.iter_lines():
                if line.startswith("data: "):
                    item = json.loads(line[6:])
                    if "_id" in item:
                        sse_event = item
                        break
        sse_thread.join(timeout=5)
        assert not sse_thread_errors, sse_thread_errors
        assert sse_event is not None
        assert sse_event["payload"] == {"i": 8}

        print(
            "  server-side watch long-poll, filters, projection, truncation, "
            "NDJSON and SSE streams, generator OK"
        )
    finally:
        db.close()


def scenario_stream_concurrency(daemon: Daemon) -> None:
    """Concurrent producers and consumers against the real NDJSON stream."""
    coll = "_it_stream_concurrency"
    producers = 6
    per_producer = 100
    consumers = 6
    db = daemon.client()
    try:
        db.collection(coll).delete({}, multi=True)
    finally:
        db.close()

    produced: list[str] = []
    produced_lock = threading.Lock()
    expected_ids: set[str] | None = None
    producer_errors: list[BaseException] = []
    consumer_errors: list[BaseException] = []
    consumed: list[list[str]] = [[] for _ in range(consumers)]

    def produce(thread_id: int) -> None:
        db = daemon.client()
        try:
            producer = EventProducer(db.collection(coll))
            ids = [
                producer.emit("stress", {"thread": thread_id, "i": i})
                for i in range(per_producer)
            ]
            with produced_lock:
                produced.extend(ids)
        except BaseException as exc:  # noqa: BLE001 - report from the thread
            producer_errors.append(exc)
        finally:
            db.close()

    def consume(index: int) -> None:
        db = daemon.client()
        try:
            stream = db.collection(coll).watch_stream(
                after=None,
                limit=200,
                timeout_ms=20_000,
                heartbeat_ms=200,
                retry=False,
            )
            for event in stream:
                event_id = event.get("_id")
                if event_id:
                    consumed[index].append(event_id)
                if expected_ids is not None and expected_ids <= set(consumed[index]):
                    stream.close()
                    break
        except BaseException as exc:  # noqa: BLE001 - report from the thread
            consumer_errors.append(exc)
        finally:
            db.close()

    consumer_threads = [
        threading.Thread(target=consume, args=(index,), name=f"stream-{index}")
        for index in range(consumers)
    ]
    for thread in consumer_threads:
        thread.start()
    time.sleep(0.2)
    with ThreadPoolExecutor(max_workers=producers) as pool:
        list(pool.map(produce, range(producers)))
    if producer_errors:
        raise producer_errors[0]

    # Wake consumers that already reached the end before `expected_ids` was set.
    expected_ids = set(produced)
    db = daemon.client()
    try:
        EventProducer(db.collection(coll)).emit(
            "stress.done", {"produced": len(expected_ids)}
        )
    finally:
        db.close()
    for thread in consumer_threads:
        thread.join(timeout=25)
    if any(thread.is_alive() for thread in consumer_threads):
        raise AssertionError("stream consumers did not finish")
    if consumer_errors:
        raise consumer_errors[0]

    for index, ids in enumerate(consumed):
        unique = set(ids)
        missing = expected_ids - unique
        duplicates = len(ids) - len(unique)
        assert not missing, (
            f"stream {index} missing {len(missing)} events: "
            f"{sorted(missing)[:5]}"
        )
        assert duplicates == 0, f"stream {index} duplicated {duplicates} events"

    db = daemon.client()
    try:
        metrics = db._http.get(db.base_url + "/metrics").json()
        assert metrics["watch_streams_total"] >= consumers
        assert metrics["watch_events_total"] >= len(expected_ids) * consumers
    finally:
        db.close()
    print(
        "  concurrent stream: "
        f"{len(expected_ids)} events, {consumers} consumers, "
        "0 missing, 0 duplicate deliveries OK"
    )


def scenario_worker_cli(daemon: Daemon) -> None:
    db = daemon.client()
    try:
        events = db.collection("_it_cli_events")
        checkpoints = db.collection("_it_cli_cp")
        clean(events, checkpoints)
        event_id = EventProducer(events).emit("order.created", {"total": 99}, doc_id="o1")
    finally:
        db.close()

    cmd = [
        sys.executable,
        "-m",
        "nonodb_worker.events_worker",
        "--module",
        "events_handlers",
        "--events",
        "_it_cli_events",
        "--checkpoints",
        "_it_cli_cp",
        "--consumer-id",
        "cli",
        "--once",
        "--log-level",
        "WARNING",
        "--socket",
        str(daemon.socket),
    ]
    result = subprocess.run(
        cmd,
        cwd=ROOT,
        env=daemon.env(ROOT / "workers" / "python" / "examples"),
        text=True,
        capture_output=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"event CLI failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    db = daemon.client()
    try:
        checkpoint = db.collection("_it_cli_cp").find_one({"_id": "cli"})
        assert checkpoint and checkpoint["last_id"] == event_id, checkpoint
    finally:
        db.close()
    print("  event worker CLI checkpoint OK")


def main() -> int:
    daemon = Daemon()
    try:
        scenarios: list[tuple[str, Callable[[Daemon], None]]] = [
            ("atomicity", scenario_atomicity),
            ("checkpoint/restart", scenario_checkpoint_and_restart),
            ("failure/retry", scenario_failure_retry),
            ("late event", scenario_late_out_of_order_event),
            ("independent consumers", scenario_independent_consumers),
            ("concurrent producers", scenario_concurrent_producers),
            ("watch stream", scenario_watch_stream),
            ("stream concurrency", scenario_stream_concurrency),
            ("worker CLI", scenario_worker_cli),
        ]
        for name, scenario in scenarios:
            print(f"[{name}]")
            scenario(daemon)
        print("events integration OK")
        return 0
    except BaseException:
        print("\ndaemon log tail:", file=sys.stderr)
        if daemon.log.exists():
            lines = daemon.log.read_text(errors="replace").splitlines()
            print("\n".join(lines[-40:]), file=sys.stderr)
        raise
    finally:
        daemon.stop()


if __name__ == "__main__":
    raise SystemExit(main())
