"""Real-daemon integration tests for outbox events.

This is intentionally not named ``test_*.py`` so the fast unit-test discovery
does not require a daemon binary. Run it explicitly:

    python3 workers/python/tests/events_integration.py

It starts ``target/debug/nonodb-daemon`` with a temporary data directory, then
exercises atomic writes, checkpoints, retries, late out-of-order events,
concurrent producers, and the event worker CLI.
"""

from __future__ import annotations

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

from nonodb import NonoDB, NonoDBError  # noqa: E402
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
