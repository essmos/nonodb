#!/usr/bin/env python3
"""Large multi-host replication load and failover benchmark.

Starts a seven-node cluster (five voters, two followers), loads a large data set
while new nodes join, measures replication convergence, runs concurrent writes,
then performs two quorum failovers and measures recovery and throughput.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import random
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/release/nonodb-daemon"))
CLI = Path(os.environ.get("NONODB_CLI", ROOT / "target/release/nonodb-cli"))

COLORS = ["amber", "blue", "cyan", "green", "red", "violet", "white"]


def free_port(preferred: int) -> int:
    for port in range(preferred, preferred + 200):
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                pass
    raise RuntimeError("no free port")


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * (len(ordered) - 1)))] * 1000.0


def make_doc(index: int) -> dict:
    return {
        "_id": f"d{index:08d}",
        "tenant": f"t{index % 100:03d}",
        "n": index,
        "even": index % 2 == 0,
        "color": COLORS[index % len(COLORS)],
        "payload": f"payload-{index}",
    }


class Node:
    def __init__(self, name: str, port: int, sock: str, data: str):
        self.name = name
        self.port = port
        self.sock = sock
        self.data = data
        self.proc: subprocess.Popen | None = None
        self.client: httpx.Client | None = None
        self.url = f"http://127.0.0.1:{port}"

    def health(self) -> bool:
        if self.client is None:
            return False
        try:
            return self.client.get("/healthz", timeout=1.0).status_code == 200
        except httpx.HTTPError:
            return False

    def close(self) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", type=int, default=1_000_000)
    parser.add_argument("--batch", type=int, default=1000)
    parser.add_argument("--write-seconds", type=float, default=10.0)
    parser.add_argument("--writers", type=int, default=8)
    parser.add_argument("--query-samples", type=int, default=300)
    parser.add_argument("--gui-port", type=int, default=8791)
    parser.add_argument("--replication-batch", type=int, default=10)
    parser.add_argument("--min-available-mb", type=int, default=1200)
    parser.add_argument("--max-node-rss-mb", type=int, default=2500)
    parser.add_argument("--max-total-rss-mb", type=int, default=6000)
    parser.add_argument("--keep", action="store_true", help="leave the cluster and GUI running")
    args = parser.parse_args()
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing release binaries; run cargo build --release --locked --bins")

    temp = Path(tempfile.mkdtemp(prefix="nonodb-load-", dir="/var/tmp"))
    auth = temp / "auth.json"
    nodes: dict[str, Node] = {}
    procs: list[subprocess.Popen] = []
    start_errors: list[str] = []
    lock = threading.Lock()
    memory_pressure = threading.Event()
    memory_messages: list[str] = []
    peak_rss: dict[str, float] = {}
    rnd = random.Random(20261114)
    results: dict = {"config": vars(args), "phases": {}}

    env_auth = {**os.environ, "NONODB_AUTH_FILE": str(auth)}
    subprocess.run([str(CLI), "auth", "init"], env=env_auth, check=True, capture_output=True)
    token = subprocess.run(
        [str(CLI), "auth", "token", "create", "admin"],
        env=env_auth,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    headers = {"Authorization": f"Bearer {token}"}

    ports = {
        "primary": free_port(7901),
        "candidate1": free_port(7911),
        "candidate2": free_port(7921),
        "candidate3": free_port(7931),
        "candidate4": free_port(7941),
        "follower1": free_port(7951),
        "follower2": free_port(7961),
    }
    voters = [
        f"http://127.0.0.1:{ports[name]}"
        for name in ("primary", "candidate1", "candidate2", "candidate3", "candidate4")
    ]
    voter_spec = ",".join(voters)
    followers = [
        f"http://127.0.0.1:{ports[name]}" for name in ("follower1", "follower2")
    ]
    follower_spec = ",".join(followers)

    def available_mb() -> float:
        try:
            for line in Path("/proc/meminfo").read_text().splitlines():
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / 1024.0
        except OSError:
            pass
        return 0.0

    def rss_mb(pid: int) -> float:
        try:
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
        except OSError:
            pass
        return 0.0

    def check_memory() -> None:
        if memory_pressure.is_set():
            raise RuntimeError(f"memory guard: {memory_messages[-1]}")

    def start_node(name: str, role: str, extra: dict[str, str]) -> Node:
        with lock:
            if name in nodes:
                return nodes[name]
            node = Node(
                name,
                ports[name],
                str(temp / f"{name}.sock"),
                str(temp / f"{name}-data"),
            )
            nodes[name] = node
        Path(node.data).mkdir()
        env = os.environ.copy()
        env.update(
            NONODB_DATA=node.data,
            NONODB_SOCK=node.sock,
            NONODB_TCP=f"127.0.0.1:{node.port}",
            NONODB_AUTH_FILE=str(auth),
            NONODB_LOG="warn",
            NONODB_REPLICATION_ENABLED="1",
            NONODB_REPLICATION_ORIGIN=f"{name}-origin",
            NONODB_REPLICATION_ROLE=role,
            NONODB_REPLICATION_NODE_ID=name,
            NONODB_REPLICATION_VOTERS=voter_spec,
            NONODB_REPLICATION_VOTER_TOKEN=token,
            NONODB_REPLICATION_FOLLOWERS=follower_spec,
            NONODB_REPLICATION_LEASE_INTERVAL="2",
            NONODB_REPLICATION_LEASE_TTL="600",
            NONODB_REPLICATION_AUTO_FAILOVER="0",
            NONODB_REPLICATION_CANDIDATE="0",
            NONODB_REPLICATION_ELECTION_INTERVAL="1",
            NONODB_REPLICATION_FAILOVER_TIMEOUT="10",
            NONODB_REPLICATION_BATCH=str(args.replication_batch),
        )
        env.update(extra)
        log = open(temp / f"{name}.log", "wb")
        proc = subprocess.Popen(
            [str(DAEMON)],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        with lock:
            node.proc = proc
            procs.append(proc)
            log.close()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f"{name} exited with {proc.returncode}")
            client = httpx.Client(base_url=node.url, timeout=120.0)
            try:
                if client.get("/healthz", timeout=1.0).status_code == 200:
                    node.client = client
                    return node
            except httpx.HTTPError:
                pass
            client.close()
            time.sleep(0.05)
        raise RuntimeError(f"{name} did not become healthy")

    start_queue: queue.Queue = queue.Queue()

    def start_worker() -> None:
        while True:
            item = start_queue.get()
            if item is None:
                return
            name, role, extra = item
            try:
                start_node(name, role, extra)
            except Exception as exc:  # noqa: BLE001
                start_errors.append(f"{name}: {exc}")

    start_worker_thread = threading.Thread(target=start_worker, daemon=True)
    start_worker_thread.start()

    def exec_node(node: Node, body: dict, timeout: float = 300.0) -> dict:
        assert node.client is not None
        response = node.client.post("/v1/exec", headers=headers, json=body, timeout=timeout)
        if response.status_code != 200:
            raise RuntimeError(f"{node.name}: {response.status_code} {response.text[:300]}")
        payload = response.json()
        if payload.get("ok") is False:
            raise RuntimeError(f"{node.name}: {payload.get('error')}")
        return payload

    def count_node(node: Node, timeout: float = 120.0) -> int:
        payload = exec_node(
            node, {"op": "count", "coll": "loadtest", "filter": {}}, timeout=timeout
        )
        return int(payload["result"]["count"])

    def status_node(node: Node) -> dict:
        assert node.client is not None
        response = node.client.get("/v1/replication/status", headers=headers, timeout=10.0)
        response.raise_for_status()
        payload = response.json()
        return payload.get("result", payload)

    def metrics_node(node: Node) -> dict:
        assert node.client is not None
        response = node.client.get("/metrics", headers=headers, timeout=10.0)
        response.raise_for_status()
        return response.json()

    def wait_role(node: Node, role: str, timeout: float) -> tuple[float, dict]:
        started = time.monotonic()
        deadline = started + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                last = status_node(node)
                if last.get("role") == role:
                    return time.monotonic() - started, last
            except (httpx.HTTPError, RuntimeError):
                pass
            time.sleep(0.1)
        raise RuntimeError(f"{node.name} did not become {role}: {last}")

    def wait_source(node: Node, source: str, timeout: float) -> float:
        started = time.monotonic()
        deadline = started + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                last = status_node(node)
                if (last.get("replication") or {}).get("source") == source:
                    return time.monotonic() - started
            except (httpx.HTTPError, RuntimeError):
                pass
            time.sleep(0.1)
        raise RuntimeError(f"{node.name} did not switch source to {source}: {last}")

    def wait_counts(targets: list[Node], expected: int, timeout: float) -> tuple[float, dict[str, int]]:
        started = time.monotonic()
        deadline = started + timeout
        last: dict[str, int] = {}
        while time.monotonic() < deadline:
            check_memory()
            last = {}
            for node in targets:
                try:
                    last[node.name] = count_node(node, timeout=30.0)
                except (httpx.HTTPError, RuntimeError):
                    last[node.name] = -1
            if all(value == expected for value in last.values()):
                return time.monotonic() - started, last
            time.sleep(0.25)
        raise RuntimeError(f"counts did not reach {expected}: {last}")

    def start_gui() -> subprocess.Popen:
        spec = ",".join(f"{name}=http://127.0.0.1:{port}" for name, port in ports.items())
        env = os.environ.copy()
        env.update(
            NONODB_AUTH_FILE=str(auth),
            NONODB_TOKEN=token,
            NONODB_CLUSTER_TOKEN=token,
            NONODB_SOCK=nodes["candidate3"].sock
            if "candidate3" in nodes
            else str(temp / "candidate3.sock"),
        )
        log = open(temp / "gui.log", "wb")
        proc = subprocess.Popen(
            [
                str(CLI),
                "gui",
                "--host",
                "0.0.0.0",
                "--port",
                str(args.gui_port),
                "--url",
                f"http://127.0.0.1:{ports['candidate3']}",
                "--allow-remote",
                "--no-open",
                "--cluster",
                spec,
            ],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        log.close()
        procs.append(proc)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError("GUI exited during startup")
            try:
                with httpx.Client(timeout=1.0) as client:
                    if client.get(f"http://127.0.0.1:{args.gui_port}/").status_code == 200:
                        return proc
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        raise RuntimeError("GUI did not start")

    completed = False
    monitor_stop = threading.Event()
    try:
        if available_mb() < args.min_available_mb:
            raise RuntimeError(
                f"not enough free memory to start safely: {available_mb():.0f}MB"
            )
        primary = start_node(
            "primary",
            "primary",
            {"NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports['primary']}"},
        )
        exec_node(primary, {"op": "create_collection", "coll": "loadtest"})
        gui_proc = start_gui()
        monitor_stop = threading.Event()

        def monitor_memory() -> None:
            while not monitor_stop.is_set():
                available = available_mb()
                with lock:
                    current_nodes = list(nodes.values())
                total = 0.0
                largest_name = ""
                largest = 0.0
                for item in current_nodes:
                    if item.proc is None or item.proc.poll() is not None:
                        continue
                    value = rss_mb(item.proc.pid)
                    total += value
                    if value > largest:
                        largest = value
                        largest_name = item.name
                    peak_rss[item.name] = max(peak_rss.get(item.name, 0.0), value)
                if (
                    available < args.min_available_mb
                    or largest > args.max_node_rss_mb
                    or total > args.max_total_rss_mb
                ):
                    message = (
                        f"available={available:.0f}MB total_rss={total:.0f}MB "
                        f"largest={largest_name}:{largest:.0f}MB"
                    )
                    memory_messages.append(message)
                    memory_pressure.set()
                    return
                time.sleep(0.5)

        monitor_thread = threading.Thread(target=monitor_memory, daemon=True)
        monitor_thread.start()
        print(f"GUI cluster view: http://10.0.0.10:{args.gui_port}/")

        # Bulk load while nodes join at different points. Starts are serialized
        # so snapshots bootstrap one at a time instead of saturating disk I/O.
        milestones = {
            int(args.docs * 0.01): [("candidate1", 0)],
            int(args.docs * 0.03): [("candidate2", 10)],
            int(args.docs * 0.05): [("candidate3", 20)],
            int(args.docs * 0.07): [("candidate4", 30)],
            int(args.docs * 0.09): [("follower1", None)],
            int(args.docs * 0.12): [("follower2", None)],
        }
        triggered: set[str] = set()

        def node_extra(name: str, priority: int | None) -> dict[str, str]:
            extra = {
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports[name]}",
            }
            if name.startswith("candidate"):
                extra.update(
                    {
                        "NONODB_REPLICATION_CANDIDATE_PRIORITY": str(priority or 0),
                        "NONODB_REPLICATION_CANDIDATE": "1",
                        "NONODB_REPLICATION_AUTO_FAILOVER": "1",
                    }
                )
            return extra

        def launch_milestone(loaded: int) -> None:
            for name, priority in milestones.get(loaded, []):
                if name in triggered:
                    continue
                triggered.add(name)
                start_queue.put((name, "replica", node_extra(name, priority)))

        load_started = time.perf_counter()
        loaded = 0
        launch_milestone(0)
        for start in range(0, args.docs, args.batch):
            documents = [
                make_doc(index)
                for index in range(start, min(start + args.batch, args.docs))
            ]
            exec_node(
                primary,
                {
                    "op": "insert_many",
                    "coll": "loadtest",
                    "docs": documents,
                },
                timeout=300.0,
            )
            loaded += len(documents)
            check_memory()
            launch_milestone(loaded)
        # Make sure every configured node exists even if a milestone was skipped.
        for name, priority in (
            ("candidate1", 0),
            ("candidate2", 10),
            ("candidate3", 20),
            ("candidate4", 30),
            ("follower1", None),
            ("follower2", None),
        ):
            if name not in triggered:
                triggered.add(name)
                start_queue.put((name, "replica", node_extra(name, priority)))
        start_queue.put(None)
        start_worker_thread.join()
        if start_errors:
            raise RuntimeError("; ".join(start_errors))
        load_elapsed = time.perf_counter() - load_started
        results["phases"]["bulk_load"] = {
            "documents": args.docs,
            "seconds": round(load_elapsed, 3),
            "docs_per_second": round(args.docs / load_elapsed, 1),
            "batch_size": args.batch,
            "durability": "full",
            "nodes_joined_during_load": [
                "candidate1",
                "candidate2",
                "candidate3",
                "candidate4",
                "follower1",
                "follower2",
            ],
        }
        replicas = [nodes[name] for name in ports if name != "primary"]
        convergence, counts = wait_counts(replicas, args.docs, timeout=900)
        results["phases"]["replication_convergence"] = {
            "documents": args.docs,
            "seconds": round(convergence, 3),
            "nodes": counts,
        }
        index_started = time.perf_counter()
        exec_node(
            primary,
            {"op": "create_index", "coll": "loadtest", "field": "tenant"},
            timeout=300.0,
        )
        index_elapsed = time.perf_counter() - index_started
        results["phases"]["index_build"] = {
            "field": "tenant",
            "seconds": round(index_elapsed, 3),
        }

        # Read latency on the primary.
        ids = [f"d{rnd.randrange(args.docs):08d}" for _ in range(args.query_samples)]
        latencies: list[float] = []
        query_started = time.perf_counter()
        for document_id in ids:
            began = time.perf_counter()
            exec_node(
                primary,
                {
                    "op": "find",
                    "coll": "loadtest",
                    "filter": {"_id": document_id},
                    "limit": 1,
                },
            )
            latencies.append(time.perf_counter() - began)
        query_elapsed = time.perf_counter() - query_started
        tenant_latencies: list[float] = []
        for index in range(max(50, args.query_samples // 2)):
            began = time.perf_counter()
            exec_node(
                primary,
                {
                    "op": "find",
                    "coll": "loadtest",
                    "filter": {"tenant": f"t{rnd.randrange(100):03d}"},
                    "limit": 10,
                },
            )
            tenant_latencies.append(time.perf_counter() - began)
        results["phases"]["read_latency"] = {
            "id_find": {
                "samples": len(latencies),
                "ops_per_second": round(len(latencies) / query_elapsed, 1),
                "p50_ms": round(pct(latencies, 0.50), 2),
                "p95_ms": round(pct(latencies, 0.95), 2),
                "p99_ms": round(pct(latencies, 0.99), 2),
            },
            "indexed_tenant_find": {
                "samples": len(tenant_latencies),
                "p50_ms": round(pct(tenant_latencies, 0.50), 2),
                "p95_ms": round(pct(tenant_latencies, 0.95), 2),
                "p99_ms": round(pct(tenant_latencies, 0.99), 2),
            },
        }

        # Concurrent writes for a fixed duration.
        write_latencies: list[float] = []
        write_errors: list[str] = []
        write_counts = [0] * args.writers
        stop = threading.Event()
        barrier = threading.Barrier(args.writers + 1)

        def writer(slot: int) -> None:
            local_latencies: list[float] = []
            with httpx.Client(base_url=primary.url, timeout=120.0) as client:
                barrier.wait()
                item = 0
                while not stop.is_set() and not memory_pressure.is_set():
                    document_id = f"cw-{slot:02d}-{item:08d}"
                    began = time.perf_counter()
                    try:
                        response = client.post(
                            "/v1/exec",
                            headers=headers,
                            json={
                                "op": "insert",
                                "coll": "loadtest",
                                "doc": make_doc(args.docs + slot * 1_000_000 + item)
                                | {"_id": document_id},
                            },
                        )
                        if response.status_code != 200:
                            write_errors.append(response.text[:200])
                        else:
                            local_latencies.append(time.perf_counter() - began)
                            write_counts[slot] += 1
                    except httpx.HTTPError as exc:
                        write_errors.append(str(exc))
                    item += 1
            with lock:
                write_latencies.extend(local_latencies)

        threads = [threading.Thread(target=writer, args=(slot,)) for slot in range(args.writers)]
        for thread in threads:
            thread.start()
        barrier.wait()
        write_started = time.perf_counter()
        time.sleep(args.write_seconds)
        stop.set()
        for thread in threads:
            thread.join()
        write_elapsed = time.perf_counter() - write_started
        written = sum(write_counts)
        check_memory()
        if write_errors:
            raise RuntimeError(f"concurrent write errors: {write_errors[:3]}")
        expected = args.docs + written
        write_convergence, write_counts_now = wait_counts(replicas, expected, timeout=600)
        results["phases"]["concurrent_writes"] = {
            "writers": args.writers,
            "seconds": round(write_elapsed, 3),
            "documents": written,
            "docs_per_second": round(written / write_elapsed, 1),
            "p50_ms": round(pct(write_latencies, 0.50), 2),
            "p95_ms": round(pct(write_latencies, 0.95), 2),
            "p99_ms": round(pct(write_latencies, 0.99), 2),
            "replication_convergence_seconds": round(write_convergence, 3),
            "nodes": write_counts_now,
        }

        # First failover: primary -> candidate1.
        primary_m = metrics_node(primary)
        assert procs and primary.proc is not None
        failover_started = time.monotonic()
        primary.proc.send_signal(signal.SIGKILL)
        primary.proc.wait(timeout=10)
        primary.close()
        c1 = nodes["candidate1"]
        election_seconds, elected = wait_role(c1, "primary", timeout=60)
        source_switch = {}
        for name in ("candidate2", "candidate3", "candidate4", "follower1", "follower2"):
            source_switch[name] = round(wait_source(nodes[name], c1.url, timeout=60), 3)
        alive = [nodes[name] for name in ("candidate1", "candidate2", "candidate3", "candidate4", "follower1", "follower2")]
        count_seconds, failover_counts = wait_counts(alive, expected, timeout=300)
        failover_total = time.monotonic() - failover_started
        results["phases"]["failover_1"] = {
            "killed": "primary",
            "new_primary": "candidate1",
            "election_seconds": round(election_seconds, 3),
            "source_switch_seconds": source_switch,
            "convergence_seconds": round(count_seconds, 3),
            "total_seconds": round(failover_total, 3),
            "epoch": elected.get("epoch"),
            "counts": failover_counts,
        }

        # Writes after failover 1.
        post_started = time.perf_counter()
        for start in range(0, 5000, 500):
            exec_node(
                c1,
                {
                    "op": "insert_many",
                    "coll": "loadtest",
                    "docs": [
                        make_doc(args.docs + 10_000_000 + index)
                        for index in range(start, start + 500)
                    ],
                },
            )
        post_elapsed = time.perf_counter() - post_started
        expected += 5000
        post_convergence, post_counts = wait_counts(alive, expected, timeout=300)
        results["phases"]["post_failover_1_writes"] = {
            "documents": 5000,
            "seconds": round(post_elapsed, 3),
            "docs_per_second": round(5000 / post_elapsed, 1),
            "replication_convergence_seconds": round(post_convergence, 3),
            "counts": post_counts,
        }

        # Second failover: candidate1 -> candidate2.
        failover2_started = time.monotonic()
        assert c1.proc is not None
        c1.proc.send_signal(signal.SIGKILL)
        c1.proc.wait(timeout=10)
        c1.close()
        c2 = nodes["candidate2"]
        election2_seconds, elected2 = wait_role(c2, "primary", timeout=60)
        source_switch2 = {}
        for name in ("candidate3", "candidate4", "follower1", "follower2"):
            source_switch2[name] = round(wait_source(nodes[name], c2.url, timeout=60), 3)
        alive2 = [nodes[name] for name in ("candidate2", "candidate3", "candidate4", "follower1", "follower2")]
        count2_seconds, failover2_counts = wait_counts(alive2, expected, timeout=300)
        failover2_total = time.monotonic() - failover2_started
        results["phases"]["failover_2"] = {
            "killed": "candidate1",
            "new_primary": "candidate2",
            "election_seconds": round(election2_seconds, 3),
            "source_switch_seconds": source_switch2,
            "convergence_seconds": round(count2_seconds, 3),
            "total_seconds": round(failover2_total, 3),
            "epoch": elected2.get("epoch"),
            "counts": failover2_counts,
        }

        # Writes after failover 2 and final query latency.
        post2_started = time.perf_counter()
        for start in range(0, 5000, 500):
            exec_node(
                c2,
                {
                    "op": "insert_many",
                    "coll": "loadtest",
                    "docs": [
                        make_doc(args.docs + 20_000_000 + index)
                        for index in range(start, start + 500)
                    ],
                },
            )
        post2_elapsed = time.perf_counter() - post2_started
        expected += 5000
        post2_convergence, post2_counts = wait_counts(alive2, expected, timeout=300)
        final_ids = [f"d{rnd.randrange(args.docs):08d}" for _ in range(args.query_samples)]
        final_latencies: list[float] = []
        for document_id in final_ids:
            began = time.perf_counter()
            exec_node(
                c2,
                {
                    "op": "find",
                    "coll": "loadtest",
                    "filter": {"_id": document_id},
                    "limit": 1,
                },
            )
            final_latencies.append(time.perf_counter() - began)
        c2_metrics = metrics_node(c2)
        results["phases"]["post_failover_2_writes"] = {
            "documents": 5000,
            "seconds": round(post2_elapsed, 3),
            "docs_per_second": round(5000 / post2_elapsed, 1),
            "replication_convergence_seconds": round(post2_convergence, 3),
            "counts": post2_counts,
        }
        results["phases"]["final_read_latency"] = {
            "samples": len(final_latencies),
            "p50_ms": round(pct(final_latencies, 0.50), 2),
            "p95_ms": round(pct(final_latencies, 0.95), 2),
            "p99_ms": round(pct(final_latencies, 0.99), 2),
        }
        results["final"] = {
            "documents": expected,
            "primary": "candidate2",
            "epoch": elected2.get("epoch"),
            "surviving_nodes": [node.name for node in alive2],
            "offline_nodes": ["primary", "candidate1"],
            "primary_metrics_before_failover": {
                key: primary_m.get(key)
                for key in ("write_ops_total", "avg_commit_us", "commit_ns_total", "writer_busy")
            },
            "candidate2_metrics": {
                key: c2_metrics.get(key)
                for key in (
                    "write_ops_total",
                    "avg_commit_us",
                    "commit_ns_total",
                    "replication_applied_total",
                    "replication_transport_errors_total",
                    "replication_apply_errors_total",
                    "requests_total",
                    "errors_total",
                )
            },
        }

        state = {
            "dir": str(temp),
            "gui_port": args.gui_port,
            "auth_file": str(auth),
            "ports": ports,
            "alive": {
                node.name: {"port": node.port, "pid": node.proc.pid}
                for node in alive2
            },
            "killed": {"primary": ports["primary"], "candidate1": ports["candidate1"]},
            "gui_pid": gui_proc.pid,
            "results": results,
        }
        (temp / "state.json").write_text(json.dumps(state, indent=2))
        results["memory"] = {
            "peak_rss_mb": {name: round(value, 1) for name, value in peak_rss.items()},
            "available_mb_at_end": round(available_mb(), 1),
        }
        (temp / "results.json").write_text(json.dumps(results, indent=2))
        print(json.dumps(results, indent=2))
        print(f"GUI cluster view: http://10.0.0.10:{args.gui_port}/")
        print(f"results: {temp / 'results.json'}")
        completed = True
        if args.keep:
            print(f"cluster kept running; state: {temp / 'state.json'}")
        else:
            print("cluster left running; pass --keep to make this explicit")
        return 0
    finally:
        monitor_stop.set()
        if not args.keep or not completed:
            for proc in procs:
                if proc.poll() is None:
                    proc.terminate()
            for proc in procs:
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
            for node in nodes.values():
                node.close()
            shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
