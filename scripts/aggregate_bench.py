"""Aggregation pipeline benchmark.

Run against a daemon (release build recommended):

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/aggregate_bench.py --docs 50000 --iters 10

The benchmark measures each supported stage separately, plus a realistic
combined pipeline. It uses one collection and keeps the data set at or below
the aggregation input cap (100,000 documents).
"""

from __future__ import annotations

import argparse
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))
    return ordered[idx] * 1000.0


def bench(
    db: NonoDB,
    label: str,
    pipeline: list[dict],
    *,
    iters: int,
    warmup: int,
) -> float:
    coll = db.collection("aggbench")
    for _ in range(warmup):
        coll.aggregate(pipeline)

    latencies: list[float] = []
    rows = 0
    for _ in range(iters):
        start = time.perf_counter()
        result = coll.aggregate(pipeline)
        latencies.append(time.perf_counter() - start)
        rows = len(result)

    p50 = pct(latencies, 0.50)
    p95 = pct(latencies, 0.95)
    print(
        f"{label:<28} rows={rows:>7}  p50={p50:8.2f}ms  "
        f"p95={p95:8.2f}ms  min={min(latencies) * 1000:8.2f}ms"
    )
    return p50


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=50_000)
    ap.add_argument("--iters", type=int, default=10)
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--chunk", type=int, default=1000)
    args = ap.parse_args()

    if args.docs < 1 or args.docs > 100_000:
        print("--docs must be between 1 and 100000 (aggregation input cap)")
        return 1
    if args.iters < 1:
        print("--iters must be positive")
        return 1

    db = NonoDB(SOCK)
    coll = db.collection("aggbench")
    coll.delete({}, multi=True)

    docs = [
        {
            "seq": i,
            "grp": i % 1000,
            "tenant": f"t{i % 100:03d}",
            "amount": (i * 37) % 1000,
            "active": i % 2 == 0,
            "tags": ["alpha", "beta"] if i % 3 else ["alpha"],
            "payload": "x" * 50,
        }
        for i in range(args.docs)
    ]
    start = time.perf_counter()
    for offset in range(0, len(docs), args.chunk):
        coll.insert_many(docs[offset : offset + args.chunk])
    load = time.perf_counter() - start
    print(
        f"docs={args.docs} iters={args.iters} warmup={args.warmup} "
        f"load={load:.2f}s ({args.docs / load:,.0f} docs/s)"
    )
    print("-" * 96)

    # Each stage is measured on its own. The data set is below the aggregation
    # input cap, so no leading $match is needed to make the pipeline legal.
    bench(
        db,
        "$match broad",
        [{"$match": {"active": True}}],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$match selective",
        [{"$match": {"seq": {"$lt": 100}}}],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$project include",
        [{"$project": {"_id": 0, "seq": 1, "tenant": 1, "amount": 1}}],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$project computed",
        [{"$project": {"_id": 0, "seq": 1, "double": {"$multiply": ["$amount", 2]}}}],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$addFields",
        [{"$addFields": {"bucket": {"$multiply": ["$grp", 10]}}}],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$unset",
        [{"$unset": ["payload", "tags"]}],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$sort",
        [{"$sort": {"amount": -1}}],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$sort + $limit",
        [{"$sort": {"amount": -1}}, {"$limit": 100}],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$skip",
        [{"$skip": args.docs // 2}],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$limit",
        [{"$limit": 100}],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$count",
        [{"$count": "rows"}],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$group sum/avg",
        [
            {"$group": {
                "_id": "$grp",
                "total": {"$sum": "$amount"},
                "avg": {"$avg": "$amount"},
                "n": {"$sum": 1},
            }}
        ],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$group push/addToSet",
        [
            {"$group": {
                "_id": "$tenant",
                "ids": {"$push": "$seq"},
                "tags": {"$addToSet": "$tags"},
            }}
        ],
        iters=args.iters,
        warmup=args.warmup,
    )
    bench(
        db,
        "$unwind",
        [{"$unwind": "$tags"}],
        iters=args.iters,
        warmup=args.warmup,
    )

    print("-" * 96)
    # A realistic report pipeline. Compare it with a leading-$match, $group-only
    # variant to show the effect of early filtering.
    combined = bench(
        db,
        "combined (filter+group)",
        [
            {"$match": {"active": True}},
            {"$group": {
                "_id": "$tenant",
                "total": {"$sum": "$amount"},
                "n": {"$sum": 1},
            }},
            {"$sort": {"total": -1}},
            {"$limit": 10},
            {"$project": {"_id": 0, "tenant": "$_id", "total": 1, "n": 1}},
        ],
        iters=args.iters,
        warmup=args.warmup,
    )
    group_only = bench(
        db,
        "group only, no match",
        [
            {"$group": {
                "_id": "$tenant",
                "total": {"$sum": "$amount"},
                "n": {"$sum": 1},
            }},
            {"$sort": {"total": -1}},
            {"$limit": 10},
        ],
        iters=args.iters,
        warmup=args.warmup,
    )
    if combined > 0:
        print(f"early $match makes this report {group_only / combined:.2f}x faster")

    print("-" * 96)
    print("Note: run the daemon with NONODB_CACHE_TTL=0 to measure execution")
    print("      rather than repeated cache hits. Timings include HTTP/UDS")
    print("      transport and JSON decoding in Python.")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
