#!/usr/bin/env python3
"""Deterministic analytics benchmark for 100k/500k/1M-node graphs.

    cargo build --release -p nonodb-daemon
    python3 scripts/graph_analytics_bench.py \
      --output docs/graph-analytics-benchmark.json

Each size uses a fixed seed. Node ids are zero-padded decimal strings and the
edge list is a sparse random graph with `edges_per_node` outgoing edges.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402

SEED = 42
BATCH = 2_000
MAX_EDGE_BUDGET_FACTOR = 8


def read_proc(path: Path, key: str) -> int | None:
    try:
        for line in path.read_text().splitlines():
            if line.startswith(key + ":"):
                value = line.split(":", 1)[1].strip().split()[0]
                return int(value)
    except OSError:
        return None
    return None


def rss_mb(pid: int, key: str = "VmRSS") -> float | None:
    kb = read_proc(Path(f"/proc/{pid}/status"), key)
    return None if kb is None else round(kb / 1024, 1)


def machine() -> dict:
    cpu = ""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    mem_kb = read_proc(Path("/proc/meminfo"), "MemTotal")
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu": cpu,
        "cpu_count": os.cpu_count(),
        "memory_total_mb": None if mem_kb is None else round(mem_kb / 1024),
    }


def chunks(items, size: int):
    batch = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def graph_options(kind: str, nodes: int, edges: int, iterations: int = 5) -> dict:
    return {
        "kind": kind,
        "direction": "both" if kind in {"k_core", "degree_distribution"} else "out",
        "limit": min(nodes, 1000),
        "max_nodes": nodes,
        "max_edges": max(edges * MAX_EDGE_BUDGET_FACTOR, 1000),
        "max_memory_bytes": 1024 * 1024 * 1024,
        "timeout_ms": 900_000,
        "iterations": max(1, iterations),
        "tolerance": 1e-6,
    }


def timed_algorithm(nodes_coll, pid: int, kind: str, n: int, m: int, iterations: int = 5) -> dict:
    before = rss_mb(pid)
    start = time.perf_counter()
    out = nodes_coll._db.exec(
        {"op": "graph_algorithm", "coll": "nodes", **graph_options(kind, n, m, iterations)},
        timeout=900,
    )
    elapsed = (time.perf_counter() - start) * 1000
    return {
        "ms": round(elapsed, 1),
        "rss_before_mb": before,
        "rss_after_mb": rss_mb(pid),
        "peak_rss_mb": rss_mb(pid, "VmHWM"),
        "result_nodes": out.get("nodes"),
    }


def timed_job(nodes_coll, pid: int, n: int, m: int) -> dict:
    before = rss_mb(pid)
    start = time.perf_counter()
    job = nodes_coll._db.exec(
        {
            "op": "start_graph_job",
            "coll": "nodes",
            **graph_options("pagerank", n, m, 5),
        },
        timeout=900,
    )
    state = "queued"
    while state not in {"completed", "failed", "cancelled"}:
        time.sleep(0.01)
        status = nodes_coll._db.exec(
            {"op": "graph_job_status", "job_id": job["job_id"]}, timeout=30
        )
        state = status["state"]
    elapsed = (time.perf_counter() - start) * 1000
    if state != "completed":
        raise RuntimeError(f"job {job['job_id']} ended in {state}: {status}")
    return {
        "ms": round(elapsed, 1),
        "rss_before_mb": before,
        "rss_after_mb": rss_mb(pid),
        "peak_rss_mb": rss_mb(pid, "VmHWM"),
        "state": state,
    }


def run_size(daemon: Path, n: int, edges_per_node: float, repeats: int) -> dict:
    m = max(n, int(n * edges_per_node))
    rng = random.Random(SEED)
    tmp = Path(tempfile.mkdtemp(prefix=f"nonodb-analytics-bench-{n}-"))
    sock = tmp / "n.sock"
    log = tmp / "daemon.log"
    env = os.environ.copy()
    env.update(
        NONODB_DATA=str(tmp / "data"),
        NONODB_SOCK=str(sock),
        NONODB_LOG="warn",
        NONODB_REQUEST_TIMEOUT="900",
    )
    logfh = log.open("wb")
    proc = subprocess.Popen([str(daemon)], env=env, stdout=logfh, stderr=subprocess.STDOUT)
    logfh.close()
    try:
        deadline = time.monotonic() + 30
        while not sock.exists() and time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f"daemon exited: {log.read_text()[-2000:]}")
            time.sleep(0.02)
        db = NonoDB(str(sock), cache_ttl=0.0)
        nodes = db.collection("nodes")
        edges = db.collection("edges")
        db.create_collection("edges")
        nodes.set_graph("edges")

        ingest_start = time.perf_counter()
        for batch in chunks(
            ({"_id": f"n{i:07}"} for i in range(n)),
            BATCH,
        ):
            nodes.insert_many(batch)
        node_ms = (time.perf_counter() - ingest_start) * 1000

        def edge_docs():
            for i in range(m):
                yield {
                    "_id": f"e{i:07}",
                    "from": f"n{rng.randrange(n):07}",
                    "to": f"n{rng.randrange(n):07}",
                }

        edge_start = time.perf_counter()
        for batch in chunks(edge_docs(), BATCH):
            edges.insert_many(batch)
        edge_ms = (time.perf_counter() - edge_start) * 1000
        ingest_ms = node_ms + edge_ms

        result = {
            "nodes": n,
            "edges": m,
            "edges_per_node": edges_per_node,
            "seed": SEED,
            "ingest": {
                "nodes_ms": round(node_ms, 1),
                "edges_ms": round(edge_ms, 1),
                "total_ms": round(ingest_ms, 1),
                "nodes_per_s": round(n / (node_ms / 1000)) if node_ms else None,
                "edges_per_s": round(m / (edge_ms / 1000)) if edge_ms else None,
            },
            "rss_after_ingest_mb": rss_mb(proc.pid),
            "peak_rss_after_ingest_mb": rss_mb(proc.pid, "VmHWM"),
            "algorithms": {},
        }
        runs: dict[str, list[dict]] = {}
        for kind, iterations in (
            ("degree_centrality", 0),
            ("degree_distribution", 0),
            ("pagerank", 5),
            ("label_propagation", 5),
            ("k_core", 0),
        ):
            runs[kind] = [
                timed_algorithm(nodes, proc.pid, kind, n, m, iterations)
                for _ in range(repeats)
            ]
            times = sorted(run["ms"] for run in runs[kind])
            result["algorithms"][kind] = {
                "p50_ms": round(statistics.median(times), 1),
                "min_ms": times[0],
                "max_ms": times[-1],
                "peak_rss_mb": max(run["peak_rss_mb"] or 0 for run in runs[kind]),
                "result_nodes": runs[kind][0]["result_nodes"],
            }
        result["pagerank_job"] = timed_job(nodes, proc.pid, n, m)
        return result
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sizes",
        default=os.environ.get("NONODB_ANALYTICS_BENCH_SIZES", "100000,500000,1000000"),
        help="comma-separated node counts",
    )
    parser.add_argument(
        "--edges-per-node",
        type=float,
        default=float(os.environ.get("NONODB_ANALYTICS_BENCH_EDGES_PER_NODE", "1.0")),
    )
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument(
        "--output",
        default=os.environ.get("NONODB_ANALYTICS_BENCH_OUTPUT", "graph-analytics-benchmark.json"),
    )
    parser.add_argument(
        "--daemon",
        default=os.environ.get("NONODB_DAEMON", str(ROOT / "target/release/nonodb-daemon")),
    )
    args = parser.parse_args()
    daemon = Path(args.daemon)
    if not daemon.is_file():
        print(f"missing benchmark daemon: {daemon}", file=sys.stderr)
        return 2
    sizes = [int(value) for value in args.sizes.split(",") if value.strip()]
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "daemon": str(daemon),
        "daemon_bytes": daemon.stat().st_size,
        "machine": machine(),
        "method": {
            "seed": SEED,
            "batch": BATCH,
            "edges_per_node": args.edges_per_node,
            "repeats": args.repeats,
            "algorithms": {
                "degree_centrality": "one pass, both directions",
                "degree_distribution": "one pass, both directions",
                "pagerank": "5 iterations, damping 0.85",
                "label_propagation": "5 synchronous iterations, both directions",
                "k_core": "peeling, both directions",
                "pagerank_job": "background job start-to-terminal, 5 iterations",
            },
            "rss": "VmRSS/VmHWM sampled from /proc before and after each operation",
        },
        "sizes": [],
    }
    for n in sizes:
        print(f"benchmarking {n} nodes...", file=sys.stderr)
        report["sizes"].append(run_size(daemon, n, args.edges_per_node, args.repeats))
        print(json.dumps(report["sizes"][-1], sort_keys=True), file=sys.stderr)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
