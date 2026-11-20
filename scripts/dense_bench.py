"""Large collection benchmark with minimal documents.

The documents contain only ``_id`` and ``n`` so a run can reach 100M or 1B
records without exhausting disk. It is intended for load, count, scan, and
point-read measurements; it does not create secondary indexes.
"""

import argparse
import os
import random
import sys
import time
from concurrent.futures import ProcessPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "")
N = 0


def ident(i):
    return f"u{i:09d}"


def doc(i):
    return {"_id": ident(i), "n": i % 1000}


def pct(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * (len(ordered) - 1)))] * 1000.0


def read_worker(seed, reps, collection):
    db = NonoDB(SOCK, timeout=60)
    coll = db.collection(collection)
    rnd = random.Random(seed)
    latencies = []
    for _ in range(reps):
        started = time.perf_counter()
        coll.find_one({"_id": ident(rnd.randrange(N))})
        latencies.append(time.perf_counter() - started)
    db.close()
    return latencies


def main():
    global N
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", type=int, default=100_000_000)
    parser.add_argument("--batch", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--reads-per-worker", type=int, default=10_000)
    parser.add_argument("--collection", default="dense")
    args = parser.parse_args()
    N = args.docs

    db = NonoDB(
        SOCK, timeout=float(os.environ.get("NONODB_TIMEOUT", "120"))
    )
    coll = db.collection(args.collection)
    if coll.count({}) > 0:
        print(f"clearing existing {args.collection!r} collection", flush=True)
        coll.delete({}, multi=True)

    started = time.perf_counter()
    last = started
    for offset in range(0, N, args.batch):
        coll.insert_many(
            [doc(i) for i in range(offset, min(offset + args.batch, N))]
        )
        done = min(offset + args.batch, N)
        if done % 10_000_000 == 0:
            now = time.perf_counter()
            rate = done / (now - started)
            print(
                f"LOAD {done:,} / {N:,}  {rate:,.0f} docs/s  "
                f"chunk={now - last:.1f}s",
                flush=True,
            )
            last = now

    load = time.perf_counter() - started
    print(
        f"LOAD_DONE {N:,} in {load:.1f}s -> {N / load:,.0f} docs/s",
        flush=True,
    )

    started = time.perf_counter()
    total = coll.count({})
    print(
        f"COUNT {total:,} in {time.perf_counter() - started:.3f}s",
        flush=True,
    )

    started = time.perf_counter()
    scanned = coll.count({"n": 0})
    scan = time.perf_counter() - started
    print(
        f"SCAN_COUNT {scanned:,} in {scan:.1f}s "
        f"({N / scan:,.0f} docs/s)",
        flush=True,
    )

    rnd = random.Random(42)
    ids = [ident(rnd.randrange(N)) for _ in range(1000)]
    latencies = []
    for key in ids:
        started = time.perf_counter()
        coll.find_one({"_id": key})
        latencies.append(time.perf_counter() - started)
    print(
        f"ID_READ_1 p50={pct(latencies, 0.5):.2f}ms "
        f"p95={pct(latencies, 0.95):.2f}ms "
        f"p99={pct(latencies, 0.99):.2f}ms",
        flush=True,
    )

    started = time.perf_counter()
    for offset in range(0, len(ids), 100):
        db.batch(
            [
                {
                    "op": "find_one",
                    "coll": args.collection,
                    "filter": {"_id": key},
                }
                for key in ids[offset : offset + 100]
            ]
        )
    elapsed = time.perf_counter() - started
    print(
        f"ID_READ_BATCH 1000 in {elapsed * 1000:.1f}ms "
        f"({len(ids) / elapsed:,.0f} ops/s)",
        flush=True,
    )

    started = time.perf_counter()
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                read_worker, 1000 + worker, args.reads_per_worker,
                args.collection,
            )
            for worker in range(args.workers)
        ]
        latencies = [latency for future in futures for latency in future.result()]
    wall = time.perf_counter() - started
    print(
        f"ID_READ_PAR {args.workers}x{args.reads_per_worker} in {wall:.1f}s "
        f"-> {len(latencies) / wall:,.0f} ops/s "
        f"p50={pct(latencies, 0.5):.2f}ms "
        f"p95={pct(latencies, 0.95):.2f}ms "
        f"p99={pct(latencies, 0.99):.2f}ms",
        flush=True,
    )
    db.close()


if __name__ == "__main__":
    main()
