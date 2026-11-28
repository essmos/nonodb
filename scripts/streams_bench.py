"""Streams/consumer-groups benchmark across nonodb, Redis Streams, and Kafka.

    # containers (or local servers) listening on their default ports
    docker run -d --name bench-redis -p 6379:6379 redis:7-alpine \
        redis-server --save '' --appendonly no
    # Kafka 3.9 in KRaft mode on 127.0.0.1:9092

    cargo build --release -p nonodb-daemon
    python3 scripts/streams_bench.py --backend nonodb --events 50000
    python3 scripts/streams_bench.py --backend redis  --events 50000
    python3 scripts/streams_bench.py --backend kafka  --events 50000

Measures three things per backend with the same payload and the same client
(Python, one connection per role, batch size 100, localhost TCP):

* `produce` — one producer, sequential appends, events/s.
* `drain`   — 1..N consumers draining a pre-filled stream, events/s end to end.
* `latency` — produce→consume p50/p99 for single events (ms).

Every backend runs the same workload, so the numbers are comparable in shape,
not as a verdict on engine internals: the client is Python (GIL bound) and the
results are dominated by round-trip cost, not by fsync policy.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

import httpx

ROOT = Path(__file__).resolve().parent.parent
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/release/nonodb-daemon"))

PAYLOAD = {
    "type": "payment",
    "account": "acc-000042",
    "amount": 1234,
    "currency": "EUR",
    "note": "benchmark payload of a realistic size, roughly 200 bytes on the wire",
}


# --------------------------------------------------------------------------- #
# nonodb
# --------------------------------------------------------------------------- #
class NonodbBackend:
    name = "nonodb"

    def __init__(self, addr: str, stream: str, batch: int, durability: str = "relaxed") -> None:
        # `addr` is the unix socket path: the TCP listener needs an auth file,
        # and the socket keeps the comparison about storage, not TLS setup.
        self.addr = addr
        self.durability = durability
        self.stream = stream
        self.batch = batch
        self._proc: subprocess.Popen[bytes] | None = None
        self._tmp: tempfile.TemporaryDirectory[str] | None = None
        self._client: httpx.Client | None = None

    def start(self) -> None:
        # The data directory must live on a real filesystem when disk I/O is
        # measured: /tmp is often tmpfs, where nothing reaches the block layer.
        root = Path(os.environ.get("NONODB_BENCH_ROOT", "/var/tmp/nonodb-bench"))
        root.mkdir(parents=True, exist_ok=True)
        self._tmp = tempfile.TemporaryDirectory(prefix="run-", dir=root)
        tmp = Path(self._tmp.name)
        sock = tmp / "nonodb.sock"
        env = os.environ.copy()
        env.update(
            {
                "NONODB_DATA": str(tmp / "data"),
                "NONODB_SOCK": str(sock),
                "NONODB_LOG": "warn",
            }
        )
        log = (tmp / "daemon.log").open("ab")
        self._proc = subprocess.Popen(
            [str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT
        )
        log.close()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if sock.exists():
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("nonodb daemon did not start")
        self._client = httpx.Client(
            transport=httpx.HTTPTransport(uds=str(sock)),
            base_url="http://localhost",
            timeout=60.0,
        )
        self.call({"op": "create_stream", "coll": self.stream})

    def stop(self) -> None:
        if self._client is not None:
            self._client.close()
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait(timeout=30)
        if self._tmp is not None:
            self._tmp.cleanup()

    def call(self, payload: dict[str, Any]) -> Any:
        assert self._client is not None
        response = self._client.post("/v1/exec", json=payload)
        body = response.json()
        if not body.get("ok"):
            raise RuntimeError(f"{payload.get('op')}: {body.get('error')}")
        return body.get("result")

    def producer(self) -> Callable[[int], None]:
        coll = self.stream

        batch = self.batch
        durability = self.durability

        def produce(count: int) -> None:
            # Batched ingestion, like the Redis pipeline and the Kafka producer
            # batches below: one request carries `batch` events, so the client
            # round trip is not the ceiling.
            for start in range(0, count, batch):
                docs = [
                    dict(PAYLOAD, i=i) for i in range(start, min(start + batch, count))
                ]
                self.call(
                    {
                        "op": "insert_many",
                        "coll": coll,
                        "docs": docs,
                        "durability": durability,
                    }
                )

        return produce

    def consumer_factory(self, group: str) -> Callable[[str], Callable[[int], int]]:
        coll = self.stream
        batch = self.batch

        def make(member: str) -> Callable[[int], int]:
            def consume(limit: int | None, stop=None, on_events=None) -> int:
                done = 0
                idle = 0
                while (limit is None or done < limit) and not (
                    stop is not None and stop.is_set()
                ):
                    batch_out = self.call(
                        {
                            "op": "watch",
                            "coll": coll,
                            "mode": "group",
                            "group": group,
                            "member": member,
                            "limit": batch if limit is None else min(batch, limit - done),
                            "timeout_ms": 100,
                        }
                    )
                    events = batch_out.get("events") or []
                    if not events:
                        idle += 1
                        if idle > 20:
                            break
                        continue
                    idle = 0
                    done += len(events)
                    if on_events is not None:
                        on_events(len(events))
                    self.call(
                        {
                            "op": "commit_stream_group",
                            "coll": coll,
                            "group": group,
                            "seq": batch_out["to_seq"],
                            "lease_id": batch_out["lease_id"],
                        }
                    )
                return done

            return consume

        return make

    def create_group(self, group: str) -> None:
        self.call(
            {
                "op": "create_stream_group",
                "coll": self.stream,
                "group": group,
                "start": "earliest",
            }
        )

    def latency_probe(self, group: str, samples: int) -> list[float]:
        coll = self.stream
        self.create_group(group)
        latencies: list[float] = []
        for i in range(samples):
            start = time.perf_counter()
            self.call({"op": "insert", "coll": coll, "doc": dict(PAYLOAD, i=i)})
            while True:
                batch = self.call(
                    {
                        "op": "watch",
                        "coll": coll,
                        "mode": "group",
                        "group": group,
                        "member": "probe",
                        "limit": 1,
                        "timeout_ms": 100,
                    }
                )
                if batch.get("events"):
                    self.call(
                        {
                            "op": "commit_stream_group",
                            "coll": coll,
                            "group": group,
                            "seq": batch["to_seq"],
                            "lease_id": batch["lease_id"],
                        }
                    )
                    break
            latencies.append((time.perf_counter() - start) * 1000)
        return latencies

    def new_stream(self, name: str) -> None:
        self.stream = name
        self.call({"op": "create_stream", "coll": name})

    def sampler(self):
        assert self._proc is not None and self._tmp is not None
        return HostProcess(self._proc.pid, Path(self._tmp.name) / "data")

    def flush(self) -> None:
        # Force the map to disk so the reported block I/O is not just page cache.
        self.call({"op": "sync"})


# --------------------------------------------------------------------------- #
# Redis Streams
# --------------------------------------------------------------------------- #
class RedisBackend:
    name = "redis"

    def __init__(self, addr: str, stream: str, batch: int, durability: str = "relaxed") -> None:
        self.addr = addr
        self.stream = stream
        self.batch = batch
        self.durability = durability
        self._redis: Any = None

    def start(self) -> None:
        import redis  # noqa: PLC0415

        host, port = self.addr.rsplit(":", 1)
        self._redis = redis.Redis(host=host, port=int(port), decode_responses=True)
        self._redis.ping()

    def stop(self) -> None:
        if self._redis is not None:
            self._redis.close()

    def producer(self) -> Callable[[int], None]:
        client = self._redis
        key = self.stream
        body = json.dumps(PAYLOAD)

        batch = self.batch

        def produce(count: int) -> None:
            for start in range(0, count, batch):
                pipe = client.pipeline(transaction=False)
                for i in range(start, min(start + batch, count)):
                    pipe.xadd(key, {"i": i, "body": body})
                pipe.execute()

        return produce

    def consumer_factory(self, group: str) -> Callable[[str], Callable[[int], int]]:
        key = self.stream
        batch = self.batch

        def make(member: str) -> Callable[[int], int]:
            client = self._redis
            # One connection per consumer, like the other backends.
            client = client.connection_pool

            def consume(limit: int | None, stop=None, on_events=None) -> int:
                import redis  # noqa: PLC0415

                host, port = self.addr.rsplit(":", 1)
                conn = redis.Redis(host=host, port=int(port), decode_responses=True)
                done = 0
                idle = 0
                try:
                    while (limit is None or done < limit) and not (
                        stop is not None and stop.is_set()
                    ):
                        resp = conn.xreadgroup(
                            group,
                            member,
                            {key: ">"},
                            count=batch if limit is None else min(batch, limit - done),
                            block=100,
                        )
                        if not resp:
                            idle += 1
                            if idle > 20:
                                break
                            continue
                        idle = 0
                        ids = []
                        for _stream, entries in resp:
                            for entry_id, _fields in entries:
                                ids.append(entry_id)
                        done += len(ids)
                        if on_events is not None and ids:
                            on_events(len(ids))
                        if ids:
                            conn.xack(key, group, *ids)
                finally:
                    conn.close()
                return done

            _ = client
            return consume

        return make

    def create_group(self, group: str) -> None:
        import redis  # noqa: PLC0415

        try:
            self._redis.xgroup_create(self.stream, group, id="0", mkstream=True)
        except redis.ResponseError as error:  # pragma: no cover - rerun
            if "BUSYGROUP" not in str(error):
                raise

    def latency_probe(self, group: str, samples: int) -> list[float]:
        import redis  # noqa: PLC0415

        host, port = self.addr.rsplit(":", 1)
        conn = redis.Redis(host=host, port=int(port), decode_responses=True)
        self.create_group(group)
        body = json.dumps(PAYLOAD)
        latencies: list[float] = []
        try:
            for i in range(samples):
                start = time.perf_counter()
                conn.xadd(self.stream, {"i": i, "body": body})
                resp = conn.xreadgroup(group, "probe", {self.stream: ">"}, count=1, block=100)
                assert resp, "redis returned no entry"
                entry_id = resp[0][1][0][0]
                conn.xack(self.stream, group, entry_id)
                latencies.append((time.perf_counter() - start) * 1000)
        finally:
            conn.close()
        return latencies

    def new_stream(self, name: str) -> None:
        self.stream = name
        self._redis.delete(name)

    def sampler(self):
        return DockerContainer("bench-redis", "/data")

    def flush(self) -> None:
        info = self._redis.info("persistence")
        if info.get("aof_enabled") or info.get("rdb_bgsave_in_progress") is not None:
            try:
                self._redis.bgsave()
            except Exception:  # noqa: BLE001 - persistence may be disabled
                pass


# --------------------------------------------------------------------------- #
# Kafka
# --------------------------------------------------------------------------- #
class KafkaBackend:
    name = "kafka"

    def __init__(self, addr: str, stream: str, batch: int, durability: str = "relaxed") -> None:
        self.addr = addr
        self.stream = stream
        self.batch = batch
        self.acks = 1 if durability == "relaxed" else "all"
        self.durability = durability

    def start(self) -> None:
        from kafka import KafkaAdminClient, KafkaProducer  # noqa: PLC0415
        from kafka.admin import NewTopic  # noqa: PLC0415

        admin = KafkaAdminClient(bootstrap_servers=self.addr, request_timeout_ms=20000)
        try:
            admin.create_topics([NewTopic(self.stream, 1, 1)])
        except Exception:  # noqa: BLE001 - topic may exist
            pass
        admin.close()
        producer = KafkaProducer(
            bootstrap_servers=self.addr,
            linger_ms=5,
            acks=self.acks,
            batch_size=16384,
            key_serializer=str.encode,
        )
        future = producer.send(self.stream, key="warmup", value=json.dumps(PAYLOAD).encode())
        future.get(timeout=30)
        producer.close()

    def stop(self) -> None:
        pass

    def producer(self) -> Callable[[int], None]:
        from kafka import KafkaProducer  # noqa: PLC0415

        producer = KafkaProducer(
            bootstrap_servers=self.addr,
            linger_ms=5,
            acks=self.acks,
            batch_size=16384,
        )
        topic = self.stream
        body = json.dumps(PAYLOAD).encode()

        batch = self.batch

        def produce(count: int) -> None:
            _ = batch
            for _ in range(count):
                producer.send(topic, value=body)
            producer.flush()

        return produce

    def consumer_factory(self, group: str) -> Callable[[str], Callable[[int], int]]:
        from kafka import KafkaConsumer  # noqa: PLC0415

        topic = self.stream
        batch = self.batch

        def make(member: str) -> Callable[[int], int]:
            def consume(limit: int | None, stop=None, on_events=None) -> int:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.addr,
                    group_id=group,
                    client_id=member,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    max_poll_records=batch,
                    fetch_min_bytes=1,
                    fetch_max_wait_ms=100,
                    value_deserializer=None,
                )
                done = 0
                idle = 0
                try:
                    while (limit is None or done < limit) and not (
                        stop is not None and stop.is_set()
                    ):
                        batches = consumer.poll(
                            timeout_ms=100,
                            max_records=batch if limit is None else min(batch, limit - done),
                        )
                        if not batches:
                            # Empty polls happen while the group joins or the
                            # fetch crosses a round trip; only give up after a
                            # few seconds of silence.
                            idle += 1
                            if idle > 50:
                                break
                            continue
                        idle = 0
                        for records in batches.values():
                            done += len(records)
                        if on_events is not None:
                            on_events(sum(len(records) for records in batches.values()))
                        consumer.commit()
                finally:
                    consumer.close()
                return done

            return consume

        return make

    def create_group(self, group: str) -> None:
        _ = group  # offsets are created on first consume

    def latency_probe(self, group: str, samples: int) -> list[float]:
        from kafka import KafkaConsumer, KafkaProducer  # noqa: PLC0415

        producer = KafkaProducer(
            bootstrap_servers=self.addr, linger_ms=0, acks=1, batch_size=0
        )
        consumer = KafkaConsumer(
            self.stream,
            bootstrap_servers=self.addr,
            group_id=group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=1,
            fetch_min_bytes=1,
            fetch_max_wait_ms=1,
        )
        consumer.poll(timeout_ms=100)  # join the group first
        body = json.dumps(PAYLOAD).encode()
        latencies: list[float] = []
        try:
            for _ in range(samples):
                start = time.perf_counter()
                producer.send(self.stream, value=body).get(timeout=30)
                while True:
                    batches = consumer.poll(timeout_ms=100, max_records=1)
                    if batches:
                        consumer.commit()
                        break
                latencies.append((time.perf_counter() - start) * 1000)
        finally:
            consumer.close()
            producer.close()
        return latencies

    def sampler(self):
        return DockerContainer("bench-kafka", "/tmp/kraft-logs")

    def flush(self) -> None:
        # Kafka flushes from its own log thread (log.flush.* settings); there is
        # no per-client fsync hook, so the numbers stay as observed.
        pass

    def new_stream(self, name: str) -> None:
        from kafka import KafkaAdminClient  # noqa: PLC0415
        from kafka.admin import NewTopic  # noqa: PLC0415

        self.stream = name
        admin = KafkaAdminClient(bootstrap_servers=self.addr, request_timeout_ms=20000)
        try:
            admin.create_topics([NewTopic(name, max(1, getattr(self, "partitions", 1)), 1)])
        except Exception:  # noqa: BLE001 - topic may exist
            pass
        admin.close()



# --------------------------------------------------------------------------- #
# resource sampling
# --------------------------------------------------------------------------- #
class HostProcess:
    """CPU, RSS and block I/O of a daemon process, plus its on-disk footprint."""

    def __init__(self, pid: int, data_dir: Path) -> None:
        self.pid = pid
        self.data_dir = data_dir

    def snapshot(self) -> dict[str, float]:
        stat = Path(f"/proc/{self.pid}/stat").read_text().split()
        cpu = (int(stat[13]) + int(stat[14])) / os.sysconf("SC_CLK_TCK")
        rss = 0
        for line in Path(f"/proc/{self.pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1]) * 1024
        read_bytes = write_bytes = 0
        try:
            for line in Path(f"/proc/{self.pid}/io").read_text().splitlines():
                key, value = line.split(":")
                if key == "read_bytes":
                    read_bytes = int(value)
                elif key == "write_bytes":
                    write_bytes = int(value)
        except OSError:
            pass
        return {
            "cpu_s": cpu,
            "rss_bytes": rss,
            "read_bytes": read_bytes,
            "write_bytes": write_bytes,
        }

    def disk_bytes(self) -> int:
        out = subprocess.run(
            ["du", "-s", "--block-size=1", str(self.data_dir)],
            capture_output=True,
            text=True,
            check=False,
        )
        try:
            return int(out.stdout.split()[0])
        except (IndexError, ValueError):
            return 0


class DockerContainer:
    """Same numbers for a container, read from its cgroup and `du` inside it."""

    def __init__(self, name: str, data_dir: str) -> None:
        self.name = name
        self.data_dir = data_dir
        container = subprocess.run(
            ["docker", "inspect", "-f", "{{.Id}}", name],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        matches = sorted(Path("/sys/fs/cgroup").glob(f"**/docker-{container}*.scope"))
        self.cgroup = matches[0] if matches else None

    def _read(self, name: str) -> str:
        assert self.cgroup is not None
        try:
            return (self.cgroup / name).read_text()
        except OSError:
            return ""

    def snapshot(self) -> dict[str, float]:
        rss = 0
        try:
            rss = int(self._read("memory.current").strip() or 0)
        except ValueError:
            pass
        cpu = 0.0
        for line in self._read("cpu.stat").splitlines():
            if line.startswith("usage_usec"):
                cpu = int(line.split()[1]) / 1_000_000
        read_bytes = write_bytes = 0
        for line in self._read("io.stat").splitlines():
            for field in line.split()[1:]:
                key, _, value = field.partition("=")
                if key == "rbytes":
                    read_bytes += int(value)
                elif key == "wbytes":
                    write_bytes += int(value)
        return {
            "cpu_s": cpu,
            "rss_bytes": rss,
            "read_bytes": read_bytes,
            "write_bytes": write_bytes,
        }

    def disk_bytes(self) -> int:
        out = subprocess.run(
            ["docker", "exec", self.name, "du", "-sb", self.data_dir],
            capture_output=True,
            text=True,
            check=False,
        )
        try:
            return int(out.stdout.split()[0])
        except (IndexError, ValueError):
            return 0


class Phase:
    """Wraps one measured phase and records its resource footprint."""

    def __init__(
        self, sampler: HostProcess | DockerContainer, flush: Callable[[], None] | None = None
    ) -> None:
        self.sampler = sampler
        self.flush = flush

    def __enter__(self) -> "Phase":
        self.before = self.sampler.snapshot()
        self.disk_before = self.sampler.disk_bytes()
        self.peak_rss = self.before["rss_bytes"]
        self._stop = threading.Event()
        self._samples: list[int] = []

        def sample() -> None:
            while not self._stop.is_set():
                try:
                    rss = self.sampler.snapshot()["rss_bytes"]
                except OSError:
                    break
                self._samples.append(rss)
                self._stop.wait(0.05)

        self._thread = threading.Thread(target=sample, daemon=True)
        self._thread.start()
        self.started = time.perf_counter()
        return self

    def __exit__(self, *exc: object) -> None:
        self.elapsed = time.perf_counter() - self.started
        self._stop.set()
        self._thread.join(timeout=1)
        self.after = self.sampler.snapshot()
        self.disk_after = self.sampler.disk_bytes()
        # Flush after taking the timing window, then re-read the counters so
        # deferred writes are attributed to this phase.
        if self.flush is not None:
            try:
                self.flush()
            except Exception:  # noqa: BLE001 - flushing is best effort
                pass
            time.sleep(0.2)
            self.after = self.sampler.snapshot()

    def result(self) -> dict[str, float]:
        samples = self._samples or [self.after["rss_bytes"]]
        cpu = self.after["cpu_s"] - self.before["cpu_s"]
        return {
            "seconds": round(self.elapsed, 3),
            "peak_rss_mb": round(max(samples) / 1024 / 1024, 1),
            "cpu_seconds": round(cpu, 2),
            "cpu_pct": round(100 * cpu / self.elapsed, 1) if self.elapsed else 0,
            "disk_read_mb": round(
                (self.after["read_bytes"] - self.before["read_bytes"]) / 1024 / 1024, 1
            ),
            "disk_write_mb": round(
                (self.after["write_bytes"] - self.before["write_bytes"]) / 1024 / 1024, 1
            ),
            "data_dir_mb": round(self.disk_after / 1024 / 1024, 1),
            "data_dir_growth_mb": round(
                (self.disk_after - self.disk_before) / 1024 / 1024, 1
            ),
        }


BACKENDS = {
    "nonodb": NonodbBackend,
    "redis": RedisBackend,
    "kafka": KafkaBackend,
}


# --------------------------------------------------------------------------- #
# driver
# --------------------------------------------------------------------------- #
def rate(count: int, elapsed: float) -> float:
    return count / elapsed if elapsed > 0 else 0.0


def run_backend(
    backend: Any,
    events: int,
    consumers: list[int],
    latency_samples: int,
    verbose: bool,
) -> dict[str, Any]:
    out: dict[str, Any] = {"backend": backend.name, "events": events, "runs": []}

    def log(message: str) -> None:
        if verbose:
            print(message, file=sys.stderr)

    sampler = backend.sampler()

    # --- produce ---
    backend.new_stream(f"{backend.stream}-produce")
    produce = backend.producer()
    with Phase(sampler, backend.flush) as phase:
        produce(events)
    produce_metrics = phase.result()
    produce_elapsed = produce_metrics["seconds"]
    log(f"{backend.name}: produced {events} in {produce_elapsed:.2f}s {produce_metrics}")

    # --- drain with 1..N consumers ---
    drained: dict[int, float] = {}
    drain_metrics: dict[int, dict[str, Any]] = {}
    for width in consumers:
        backend.partitions = width
        backend.new_stream(f"{backend.stream}-drain{width}")
        group = f"bench-drain-{width}"
        backend.producer()(events)
        backend.create_group(group)
        make = backend.consumer_factory(group)
        counts = [0] * width
        stop = threading.Event()
        reached = threading.Event()
        seen = [0]
        first_at: list[float | None] = [None]
        lock = threading.Lock()

        def on_events(count: int) -> None:
            with lock:
                if first_at[0] is None:
                    first_at[0] = time.perf_counter()
                seen[0] += count
                if seen[0] >= events:
                    reached.set()

        # Race the consumers with each other through a barrier so the measured
        # window is the drain itself, not the thread start-up.
        barrier = threading.Barrier(width + 1)

        errors: list[BaseException] = []

        def worker(index: int) -> None:
            try:
                consume = make(f"c{index}")
            except BaseException as error:  # noqa: BLE001 - reported below
                errors.append(error)
                stop.set()
                return
            barrier.wait()
            try:
                counts[index] = consume(None, stop=stop, on_events=on_events)
            except BaseException as error:  # noqa: BLE001 - reported below
                errors.append(error)
                stop.set()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(width)]
        with Phase(sampler, backend.flush) as phase:
            for thread in threads:
                thread.start()
            barrier.wait()
            reached.wait(timeout=120)
            finished = time.perf_counter()
            stop.set()
            for thread in threads:
                thread.join()
        if errors:
            raise errors[0]
        metrics = phase.result()
        # Measured from the first delivered record, so consumer start-up (a Kafka
        # group join, a cold connection) is not counted as drain time.
        if first_at[0] is not None:
            metrics["seconds"] = round(finished - first_at[0], 3)
        drained[width] = rate(sum(counts), metrics["seconds"])
        metrics["events_per_second"] = round(drained[width])
        drain_metrics[width] = metrics
        log(
            f"{backend.name}: drained {sum(counts)} with {width} consumers in "
            f"{metrics['seconds']:.2f}s"
        )

    # --- one-at-a-time round trip: produce → consume → ack ---
    backend.new_stream(f"{backend.stream}-latency")
    with Phase(sampler, backend.flush) as phase:
        latencies = backend.latency_probe("bench-latency", latency_samples)
    latency_metrics = phase.result()
    latencies.sort()
    out["latency"] = {
        "samples": len(latencies),
        "p50_ms": round(statistics.median(latencies), 3),
        "p99_ms": round(latencies[min(len(latencies) - 1, int(len(latencies) * 0.99))], 3),
        "mean_ms": round(statistics.fmean(latencies), 3),
    }

    out["runs"].append(
        {
            "phase": "produce",
            "events": events,
            "events_per_second": round(rate(events, produce_elapsed)),
            **produce_metrics,
        }
    )
    for width in consumers:
        out["runs"].append(
            {
                "phase": "drain",
                "consumers": width,
                "events": events,
                **drain_metrics[width],
            }
        )
    out["latency"].update(latency_metrics)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=sorted(BACKENDS), required=True)
    parser.add_argument("--addr", default=None, help="host:port (defaults per backend)")
    parser.add_argument("--events", type=int, default=50_000)
    parser.add_argument("--consumers", default="1,2", help="comma separated widths")
    parser.add_argument("--latency-samples", type=int, default=200)
    parser.add_argument("--stream", default="bench-events")
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument(
        "--durability",
        choices=["relaxed", "full"],
        default="relaxed",
        help=(
            "nonodb insert durability / Kafka producer acks. Redis needs the "
            "matching server config (appendonly yes + appendfsync always for full)."
        ),
    )
    parser.add_argument("--json-out", default=None)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    default_addr = {
        "nonodb": "/tmp/nonodb-bench.sock",
        "redis": "127.0.0.1:6379",
        "kafka": "127.0.0.1:9092",
    }[args.backend]
    backend = BACKENDS[args.backend](
        args.addr or default_addr, args.stream, args.batch, args.durability
    )
    consumers = [int(part) for part in args.consumers.split(",") if part]
    backend.start()
    try:
        result = run_backend(
            backend, args.events, consumers, args.latency_samples, not args.quiet
        )
    finally:
        backend.stop()
    result["meta"] = {
        "python": sys.version.split()[0],
        "durability": args.durability,
        "batch": args.batch,
        "payload_bytes": len(json.dumps(PAYLOAD)),
        "addr": args.addr or default_addr,
    }
    text = json.dumps(result, indent=2)
    print(text)
    if args.json_out:
        Path(args.json_out).write_text(text + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
