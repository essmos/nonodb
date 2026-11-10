"""nonodb benchmark: write throughput (group commit) and `_id` read throughput.

Run with (requires a running daemon):
    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/bench.py --workers 12 --writes 12000 --reads 60000

Each worker is a separate process with its own UDS connection - modeled after uvicorn.
"""

from __future__ import annotations

import argparse
import os
import random
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))


def _client():
    from nonodb import NonoDB

    return NonoDB(SOCK)


def write_worker(prefix: int, n: int) -> tuple[list[str], list[float], float]:
    db = _client()
    coll = db.collection("bench")
    ids: list[str] = []
    lat: list[float] = []
    start = time.perf_counter()
    for i in range(n):
        t = time.perf_counter()
        ids.append(coll.insert({"k": f"{prefix}-{i}", "v": i, "blob": "x" * 100}))
        lat.append(time.perf_counter() - t)
    elapsed = time.perf_counter() - start
    db.close()
    # For the read phase, return only a sample of `_id`s, not the whole set.
    return ids[:500], lat, elapsed


def read_worker(ids: list[str], reps: int) -> tuple[list[float], float]:
    db = _client()
    coll = db.collection("bench")
    lat: list[float] = []
    start = time.perf_counter()
    for _ in range(reps):
        t = time.perf_counter()
        coll.find_one({"_id": random.choice(ids)})
        lat.append(time.perf_counter() - t)
    elapsed = time.perf_counter() - start
    db.close()
    return lat, elapsed


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(len(s) - 1, int(round(q * (len(s) - 1))))
    return s[idx] * 1000.0  # ms


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--writes", type=int, default=12000)
    ap.add_argument("--reads", type=int, default=60000)
    args = ap.parse_args()

    per_worker_w = args.writes // args.workers
    per_worker_r = args.reads // args.workers
    if per_worker_w == 0 or per_worker_r == 0:
        print("za mało operacji dla podanej liczby workerów")
        return 1

    # Clear the benchmark collection.
    db = _client()
    db.collection("bench").delete({}, multi=True)
    db.close()

    print(f"workers={args.workers} writes={args.writes} reads={args.reads}")

    # --- writes ---
    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = [
            ex.submit(write_worker, w, per_worker_w) for w in range(args.workers)
        ]
        results = [f.result() for f in futures]
    wall_w = time.perf_counter() - t0
    total_w = sum(len(r[1]) for r in results)
    write_lat = [x for r in results for x in r[1]]
    sample_ids = [i for r in results for i in r[0]]
    print(
        f"WRITE  {total_w:>7} ops in {wall_w:6.2f}s  ->  {total_w / wall_w:8.0f} ops/s"
        f"   p50={pct(write_lat, 0.50):.2f}ms p95={pct(write_lat, 0.95):.2f}ms"
        f" p99={pct(write_lat, 0.99):.2f}ms"
    )

    # --- reads by _id ---
    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = [
            ex.submit(read_worker, sample_ids, per_worker_r)
            for _ in range(args.workers)
        ]
        results = [f.result() for f in futures]
    wall_r = time.perf_counter() - t0
    total_r = sum(len(r[0]) for r in results)
    read_lat = [x for r in results for x in r[0]]
    print(
        f"READ   {total_r:>7} ops in {wall_r:6.2f}s  ->  {total_r / wall_r:8.0f} ops/s"
        f"   p50={pct(read_lat, 0.50):.2f}ms p95={pct(read_lat, 0.95):.2f}ms"
        f" p99={pct(read_lat, 0.99):.2f}ms"
    )

    print(f"median write latency: {statistics.median(write_lat) * 1000:.2f}ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
