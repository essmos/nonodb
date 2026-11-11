"""Benchmark ordered lookups by `_id` and a unique index.

Run against a release daemon with caching disabled:

    NONODB_CACHE_TTL=0 NONODB_DATA=/tmp/lookup-data \
      NONODB_SOCK=/tmp/lookup.sock ./target/release/nonodb-daemon

    NONODB_SOCK=/tmp/lookup.sock python3 scripts/lookup_bench.py --docs 1000000
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))
from nonodb import NonoDB  # noqa: E402


def pct(values, q):
    if not values:
        return 0.0
    s = sorted(values)
    i = min(len(s) - 1, int(round(q * (len(s) - 1))))
    return s[i] * 1000.0


def summarize(name, latencies, keys):
    total = sum(latencies)
    return {
        "name": name,
        "requests": len(latencies),
        "keys": keys,
        "total_s": total,
        "req_per_s": len(latencies) / total if total else 0.0,
        "keys_per_s": keys / total if total else 0.0,
        "p50_ms": pct(latencies, 0.50),
        "p95_ms": pct(latencies, 0.95),
        "p99_ms": pct(latencies, 0.99),
    }


def run_case(name, fn, keys_per_req, warm, reps):
    for _ in range(warm):
        fn()
    lat = []
    for _ in range(reps):
        start = time.perf_counter()
        fn()
        lat.append(time.perf_counter() - start)
    return summarize(name, lat, keys_per_req * reps)


def values_for(kind, n, docs, missing_rate, rng):
    present = int(round(n * (1.0 - missing_rate)))
    idxs = rng.sample(range(docs), present)
    if kind == "id":
        values = [f"u{i:07d}" for i in idxs]
    else:
        values = [f"user{i:07d}@example.com" for i in idxs]
    for j in range(n - present):
        values.append(f"missing-{kind}-{rng.randrange(1_000_000_000)}-{j}")
    rng.shuffle(values)
    return values


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=1_000_000)
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--missing", type=float, default=0.20)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    db = NonoDB(os.environ["NONODB_SOCK"], cache_ttl=0.0)
    c = db.collection("lookup_bench")
    c.delete({}, multi=True)

    t0 = time.perf_counter()
    for start in range(0, args.docs, args.batch):
        end = min(args.docs, start + args.batch)
        docs = [
            {
                "_id": f"u{i:07d}",
                "email": f"user{i:07d}@example.com",
                "name": f"User {i}",
                "status": "active" if i % 3 else "inactive",
                "tenant": i % 1000,
                "n": i,
                "blob": "x" * 80,
            }
            for i in range(start, end)
        ]
        c.insert_many(docs)
    load_s = time.perf_counter() - t0
    print(f"LOAD {args.docs} docs in {load_s:.2f}s = {args.docs/load_s:.0f} docs/s", flush=True)

    t0 = time.perf_counter()
    c.create_index("email", unique=True, include=["name", "status"])
    index_s = time.perf_counter() - t0
    print(f"INDEX unique(email) include(name,status) in {index_s:.2f}s = {args.docs/index_s:.0f} docs/s", flush=True)

    sizes = [1, 100, 1000]
    cases = []
    value_sets = {}
    for n in sizes:
        value_sets[("id", n)] = values_for("id", n, args.docs, args.missing, rng)
        value_sets[("email", n)] = values_for("email", n, args.docs, args.missing, rng)

    for n in sizes:
        idv = value_sets[("id", n)]
        emailv = value_sets[("email", n)]
        reps = {1: 500, 100: 300, 1000: 150}[n]
        cases.append(run_case(
            f"lookup _id full n={n}", lambda v=idv: c.lookup("_id", v), n, 30, reps
        ))
        cases.append(run_case(
            f"lookup email covering n={n}", lambda v=emailv: c.lookup("email", v, fields=["name", "status"]), n, 30, reps
        ))
        cases.append(run_case(
            f"lookup email full n={n}", lambda v=emailv: c.lookup("email", v), n, 20, reps
        ))
        if n in (100, 1000):
            cases.append(run_case(
                f"batch find_one _id n={n}",
                lambda v=idv: db.batch([
                    {"op": "find_one", "coll": c.name, "filter": {"_id": x}} for x in v
                ]),
                n,
                10,
                {100: 100, 1000: 60}[n],
            ))
        if n == 1000:
            duplicate_ids = (idv[:500] * 2)
            cases.append(run_case(
                "lookup _id full n=1000, 50% duplicates",
                lambda v=duplicate_ids: c.lookup("_id", v),
                n,
                30,
                150,
            ))

    print("\nRESULTS")
    for row in cases:
        print(json.dumps(row, sort_keys=True))

    data = os.path.join(os.environ["NONODB_DATA"], "data.mdb")
    print(f"\nDATA {data} {os.path.getsize(data)/1024/1024:.1f} MiB")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
