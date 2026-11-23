#!/usr/bin/env python3
"""SQL benchmark against a running NonoDB daemon.

Run a release build for representative numbers:

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/sql_bench.py --docs 100000 --iters 20
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round(quantile * (len(ordered) - 1))))
    return ordered[index] * 1000.0


def bench(db: NonoDB, label: str, query: str, params: list, *, iters: int, warmup: int) -> dict:
    for _ in range(warmup):
        db.sql(query, params)
    latencies: list[float] = []
    rows = 0
    for _ in range(iters):
        start = time.perf_counter()
        result = db.sql(query, params)
        latencies.append(time.perf_counter() - start)
        rows = len(result)
    result = {
        "query": label,
        "rows": rows,
        "p50_ms": round(percentile(latencies, 0.50), 3),
        "p95_ms": round(percentile(latencies, 0.95), 3),
        "min_ms": round(min(latencies) * 1000.0, 3),
    }
    print(
        f"{label:<34} rows={rows:>7}  p50={result['p50_ms']:8.2f}ms  "
        f"p95={result['p95_ms']:8.2f}ms  min={result['min_ms']:8.2f}ms"
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", type=int, default=50_000)
    parser.add_argument("--iters", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--chunk", type=int, default=1_000)
    parser.add_argument("--json", action="store_true", help="emit only the JSON summary")
    args = parser.parse_args()
    if args.docs < 1 or args.iters < 1 or args.chunk < 1:
        print("--docs, --iters, and --chunk must be positive", file=sys.stderr)
        return 2

    db = NonoDB(SOCK, cache_ttl=0.0)
    collection = db.collection("sqlbench")
    collection.delete({}, multi=True)
    docs = [
        {
            "seq": i,
            "name": f"user{i:07d}",
            "team": f"t{i % 100:03d}",
            "age": 18 + (i % 60),
            "score": i % 1000,
            "active": i % 2 == 0,
        }
        for i in range(args.docs)
    ]
    started = time.perf_counter()
    for offset in range(0, len(docs), args.chunk):
        collection.insert_many(docs[offset : offset + args.chunk])
    load_seconds = time.perf_counter() - started
    collection.create_index("seq")
    collection.create_index("team")

    results = [
        bench(
            db,
            "COUNT(*)",
            "SELECT COUNT(*) AS n FROM sqlbench",
            [],
            iters=args.iters,
            warmup=args.warmup,
        ),
        bench(
            db,
            "GROUP BY team",
            "SELECT team, COUNT(*) AS n, SUM(score) AS total FROM sqlbench "
            "GROUP BY team ORDER BY team",
            [],
            iters=args.iters,
            warmup=args.warmup,
        ),
        bench(
            db,
            "DISTINCT aggregates",
            "SELECT COUNT(DISTINCT score) AS n, SUM(DISTINCT score) AS total, "
            "AVG(DISTINCT score) AS average, MIN(DISTINCT score) AS smallest, "
            "MAX(DISTINCT score) AS largest FROM sqlbench",
            [],
            iters=args.iters,
            warmup=args.warmup,
        ),
        bench(
            db,
            "indexed equality",
            "SELECT name, age, team FROM sqlbench WHERE seq = $1",
            [args.docs // 2],
            iters=args.iters,
            warmup=args.warmup,
        ),
        bench(
            db,
            "filtered aggregate",
            "SELECT COUNT(*) AS n, AVG(age) AS average FROM sqlbench "
            "WHERE age >= $1 AND active = TRUE",
            [40],
            iters=args.iters,
            warmup=args.warmup,
        ),
    ]
    summary = {
        "docs": args.docs,
        "load_seconds": round(load_seconds, 3),
        "load_docs_per_second": round(args.docs / load_seconds, 1),
        "benchmarks": results,
    }
    if args.json:
        print(json.dumps(summary, sort_keys=True))
    else:
        print(
            f"docs={args.docs} load={load_seconds:.2f}s "
            f"({args.docs / load_seconds:,.0f} docs/s)"
        )
        print(json.dumps(summary, sort_keys=True))
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
