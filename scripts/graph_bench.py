#!/usr/bin/env python3
"""Small deterministic graph benchmark (ingest, traversal, shortest, hybrid)."""

from __future__ import annotations

import json
import os
import random
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))
from nonodb import NonoDB  # noqa: E402


def timed(fn, repeat: int = 7) -> dict[str, float]:
    values = []
    for _ in range(repeat):
        start = time.perf_counter()
        fn()
        values.append((time.perf_counter() - start) * 1000)
    values.sort()
    return {
        "p50_ms": round(statistics.median(values), 3),
        "p95_ms": round(values[min(len(values) - 1, int(len(values) * 0.95))], 3),
        "min_ms": round(values[0], 3),
    }


def main() -> int:
    release = ROOT / "target/release/nonodb-daemon"
    daemon = Path(
        os.environ.get("NONODB_DAEMON", release if release.is_file() else ROOT / "target/debug/nonodb-daemon")
    )
    if not daemon.is_file():
        raise SystemExit("missing daemon binary; run cargo build --release --bins")
    n = int(os.environ.get("NONODB_BENCH_NODES", "2000"))
    m = int(os.environ.get("NONODB_BENCH_EDGES", "8000"))
    rng = random.Random(42)
    with tempfile.TemporaryDirectory(prefix="nonodb-graph-bench-") as td:
        tmp = Path(td)
        sock = tmp / "n.sock"
        env = os.environ.copy()
        env.update(NONODB_DATA=str(tmp / "data"), NONODB_SOCK=str(sock), NONODB_LOG="warn")
        proc = subprocess.Popen([str(daemon)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 15
            while not sock.exists() and time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError("daemon exited")
                time.sleep(0.02)
            db = NonoDB(str(sock), cache_ttl=0.0)
            nodes = db.collection("nodes")
            nodes.insert_many(
                [
                    {"_id": f"n{i}", "embedding": [rng.random(), rng.random(), rng.random()]}
                    for i in range(n)
                ]
            )
            edges = [{"_id": f"e{i}", "from": f"n{rng.randrange(n)}", "to": f"n{rng.randrange(n)}", "weight": 1} for i in range(m)]

            plain = db.collection("plain_edges")
            db.create_collection("plain_edges")
            start = time.perf_counter()
            plain.insert_many(edges)
            ingest_plain_ms = (time.perf_counter() - start) * 1000

            db.create_collection("edges")
            adj = db.collection("edges")
            nodes.set_graph("edges")
            start = time.perf_counter()
            adj.insert_many(edges)
            ingest_adj_ms = (time.perf_counter() - start) * 1000

            starts = [f"n{rng.randrange(n)}" for _ in range(20)]
            depth1 = timed(lambda: [nodes.traverse(s, max_depth=1, tree=False) for s in starts[:5]])
            depth3 = timed(lambda: [nodes.traverse(s, max_depth=3, tree=False) for s in starts[:5]])
            shortest = timed(
                lambda: [
                    nodes.shortest_path(f"n{rng.randrange(n)}", f"n{rng.randrange(n)}", max_depth=4)
                    for _ in range(10)
                ]
            )
            nodes.create_vector_index("embedding", 3)
            hybrid = timed(
                lambda: nodes.hybrid_search(
                    "embedding", [0.5, 0.5, 0.5], k=10, max_depth=2, limit=20
                )
            )
            reindex = timed(lambda: nodes.reindex_graph(online=True), repeat=1)
            print(
                json.dumps(
                    {
                        "nodes": n,
                        "edges": m,
                        "ingest_plain_edges_per_s": round(m / (ingest_plain_ms / 1000)),
                        "ingest_adjacency_edges_per_s": round(m / (ingest_adj_ms / 1000)),
                        "ingest_adjacency_cost_x": round(ingest_adj_ms / ingest_plain_ms, 2),
                        "traverse_depth1": depth1,
                        "traverse_depth3": depth3,
                        "shortest_path_bfs": shortest,
                        "hybrid_rerank": hybrid,
                        "online_rebuild_start": reindex,
                    },
                    sort_keys=True,
                )
            )
            return 0
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
