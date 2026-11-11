"""Search and aggregation benchmark across BSON types.

The script creates a fresh main collection with one million typed documents and
a smaller text collection, then measures representative find/filter, sort,
count, text-search, fuzzy-search, and aggregation operations.

Run against a release daemon:

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/search_aggregate_bench.py

The target database should be disposable; the script clears both collections.
Use `--skip-text` to omit text/fuzzy index builds on slower machines.
"""

from __future__ import annotations

import argparse
import base64
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
DATE0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
WORDS = [
    "database",
    "storage",
    "engine",
    "rust",
    "async",
    "search",
    "index",
    "query",
    "memory",
    "latency",
    "postgresql",
    "python",
]


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))
    return ordered[idx] * 1000.0


def run(label: str, fn, *, iters: int, warmup: int = 2) -> float | None:
    try:
        for _ in range(warmup):
            fn()
        latencies: list[float] = []
        rows = 0
        for _ in range(iters):
            start = time.perf_counter()
            result = fn()
            latencies.append(time.perf_counter() - start)
            rows = result if isinstance(result, int) else len(result)
        p50 = pct(latencies, 0.50)
        p95 = pct(latencies, 0.95)
        print(
            f"{label:<52} rows={rows:>8}  p50={p50:9.2f}ms  "
            f"p95={p95:9.2f}ms  min={min(latencies) * 1000:9.2f}ms"
        )
        return p50
    except Exception as exc:  # noqa: BLE001 - report the operation and continue
        print(f"{label:<52} ERROR {type(exc).__name__}: {exc}")
        return None


def clear(c) -> None:
    c.delete({}, multi=True)
    for ix in c.list_indexes():
        c.drop_index("+".join(ix["fields"]))
    for field in c.list_text_indexes():
        c.drop_text_index(field)
    for field in c.list_fuzzy_indexes():
        c.drop_fuzzy_index(field)


def make_doc(i: int) -> dict:
    dt = DATE0 + timedelta(minutes=i)
    return {
        "_id": f"d{i:08d}",
        "seq": i,
        "grp": i % 1000,
        "tenant": f"t{i % 100:03d}",
        "num": (i * 17) % 1_000_000,
        "dbl": i / 7.0,
        "dec": {"$numberDecimal": f"{i / 100:.2f}"},
        "i64": {"$numberLong": str(2**53 + i)},
        "str": f"user {i:08d}",
        "prefix": f"p{i % 100:03d}",
        "flag": i % 2 == 0,
        "date": {"$date": dt.isoformat()},
        "oid": {"$oid": f"{i + 1:024x}"},
        "uuid": {"$uuid": str(uuid.UUID(int=i + 1))},
        "bin": {
            "$binary": {
                "base64": base64.b64encode((i & 0xFFFF).to_bytes(4, "big")).decode(),
                "subType": "00",
            }
        },
        "tags": ["alpha", f"g{i % 100:02d}", "beta"]
        if i % 3
        else ["alpha", f"g{i % 100:02d}"],
        "scores": [i % 10, (i + 3) % 10],
        "items": [
            {"sku": f"s{i % 50}", "qty": i % 20},
            {"sku": f"s{(i + 1) % 50}", "qty": (i + 5) % 20},
        ],
        "nested": {"v": i % 10_000, "label": f"n{i % 10}", "grp": i % 50},
        **({"opt": i} if i % 10 else {}),
    }


def load_main(db: NonoDB, docs: int, chunk: int) -> float:
    c = db.collection("opsbench")
    clear(c)
    start = time.perf_counter()
    for offset in range(0, docs, chunk):
        c.insert_many(make_doc(i) for i in range(offset, min(offset + chunk, docs)))
    load = time.perf_counter() - start
    print(f"LOAD main {docs:,} docs in {load:.2f}s ({docs / load:,.0f} docs/s)")

    start = time.perf_counter()
    for field in [
        "grp",
        "num",
        "dec",
        "i64",
        "str",
        "date",
        "oid",
        "uuid",
        "flag",
        "nested.v",
    ]:
        c.create_index(field)
    index_time = time.perf_counter() - start
    print(f"INDEX 10 typed fields in {index_time:.2f}s")
    return load


def load_text(db: NonoDB, docs: int, chunk: int) -> None:
    c = db.collection("ops_text")
    clear(c)
    rnd = random.Random(42)
    start = time.perf_counter()
    for offset in range(0, docs, chunk):
        batch = []
        for i in range(offset, min(offset + chunk, docs)):
            batch.append(
                {
                    "_id": f"t{i:08d}",
                    "title": f"{rnd.choice(WORDS).title()} {rnd.choice(WORDS)} {i}",
                    "body": " ".join(rnd.choice(WORDS) for _ in range(14)),
                    "grp": i % 100,
                    "cat": f"c{i % 10}",
                }
            )
        c.insert_many(batch)
    print(f"LOAD text {docs:,} docs in {time.perf_counter() - start:.2f}s")
    start = time.perf_counter()
    c.create_index("cat")
    c.create_text_index("body")
    print(f"INDEX text body in {time.perf_counter() - start:.2f}s")
    start = time.perf_counter()
    c.create_fuzzy_index("title")
    print(f"INDEX fuzzy title in {time.perf_counter() - start:.2f}s")


def bench_find(c) -> None:
    print("\nFIND / FILTER")
    print("-" * 122)
    exact_i = 12_345
    exact_num = (exact_i * 17) % 1_000_000
    exact_dec = f"{exact_i / 100:.2f}"
    exact_str = f"user {exact_i:08d}"
    exact_date = (DATE0 + timedelta(minutes=exact_i)).isoformat()
    exact_oid = f"{exact_i + 1:024x}"
    exact_uuid = str(uuid.UUID(int=exact_i + 1))
    exact_bin = base64.b64encode((exact_i & 0xFFFF).to_bytes(4, "big")).decode()
    range_start = (DATE0 + timedelta(minutes=100_000)).isoformat()
    range_end = (DATE0 + timedelta(minutes=100_100)).isoformat()

    fast = dict(iters=200, warmup=5)
    medium = dict(iters=30, warmup=2)
    scan = dict(iters=5, warmup=1)

    run("eq int, indexed", lambda: c.find({"num": exact_num}, limit=10), **fast)
    run(
        "range int, indexed",
        lambda: c.find({"num": {"$gte": 100_000, "$lt": 100_100}}, limit=10),
        **fast,
    )
    run(
        "eq decimal, indexed",
        lambda: c.find({"dec": {"$numberDecimal": exact_dec}}, limit=10),
        **fast,
    )
    run(
        "range decimal, indexed",
        lambda: c.find(
            {
                "dec": {
                    "$gte": {"$numberDecimal": "1000.00"},
                    "$lt": {"$numberDecimal": "1001.00"},
                }
            },
            limit=10,
        ),
        **medium,
    )
    run(
        "eq long >2^53, indexed",
        lambda: c.find({"i64": {"$numberLong": str(2**53 + exact_i)}}, limit=10),
        **fast,
    )
    run("eq string, indexed", lambda: c.find({"str": exact_str}, limit=10), **fast)
    run(
        "prefix regex, string index",
        lambda: c.find({"str": {"$regex": "^user 000123"}}, limit=10),
        **medium,
    )
    run(
        "contains regex, no index",
        lambda: c.find({"str": {"$regex": "12345"}}, limit=10),
        **scan,
    )
    run(
        "eq date, indexed",
        lambda: c.find({"date": {"$date": exact_date}}, limit=10),
        **fast,
    )
    run(
        "range date, indexed",
        lambda: c.find(
            {"date": {"$gte": {"$date": range_start}, "$lt": {"$date": range_end}}},
            limit=10,
        ),
        **fast,
    )
    run("eq objectId, indexed", lambda: c.find({"oid": {"$oid": exact_oid}}, limit=10), **fast)
    run("eq uuid, indexed", lambda: c.find({"uuid": {"$uuid": exact_uuid}}, limit=10), **fast)
    run(
        "eq binary, no index",
        lambda: c.find(
            {"bin": {"$binary": {"base64": exact_bin, "subType": "00"}}}, limit=10
        ),
        **scan,
    )
    run("eq bool, indexed", lambda: c.find({"flag": True}, limit=10), **fast)
    run(
        "$in 10 ints, indexed",
        lambda: c.find(
            {
                "num": {
                    "$in": [(i * 17) % 1_000_000 for i in range(100, 110)]
                }
            },
            limit=10,
        ),
        **medium,
    )
    run(
        "$or two indexed fields",
        lambda: c.find(
            {"$or": [{"num": exact_num}, {"str": exact_str}]}, limit=10
        ),
        **medium,
    )
    run(
        "null / missing field",
        lambda: c.find({"opt": None}, limit=10),
        **scan,
    )
    run(
        "$exists true",
        lambda: c.find({"opt": {"$exists": True}}, limit=10),
        **scan,
    )
    run(
        "$all tags [alpha,beta]",
        lambda: c.find({"tags": {"$all": ["alpha", "beta"]}}, limit=10),
        **scan,
    )
    run(
        "$size tags = 3",
        lambda: c.find({"tags": {"$size": 3}}, limit=10),
        **scan,
    )
    run(
        "$elemMatch scores",
        lambda: c.find(
            {"scores": {"$elemMatch": {"$gte": 8, "$lt": 10}}}, limit=10
        ),
        **scan,
    )
    run(
        "$elemMatch objects",
        lambda: c.find(
            {"items": {"$elemMatch": {"sku": "s10", "qty": {"$gt": 5}}}},
            limit=10,
        ),
        **scan,
    )
    type_fields = {
        "int": "num",
        "long": "i64",
        "double": "dbl",
        "decimal": "dec",
        "string": "str",
        "bool": "flag",
        "date": "date",
        "objectId": "oid",
        "binData": "bin",
    }
    for type_name, field in type_fields.items():
        run(
            f"$type {type_name}",
            lambda field=field, type_name=type_name: c.find(
                {field: {"$type": type_name}}, limit=10
            ),
            **scan,
        )

    run(
        "sort indexed num desc, limit 10",
        lambda: c.find({}, sort=[("num", -1)], limit=10),
        **fast,
    )
    run(
        "range filter + indexed sort, limit 10",
        lambda: c.find({"num": {"$gte": 0}}, sort=[("num", -1)], limit=10),
        **fast,
    )
    run(
        "sort unindexed nested.label, limit 10",
        lambda: c.find({}, sort=[("nested.label", 1)], limit=10),
        **scan,
    )
    run(
        "filter grp + sort num, limit 10",
        lambda: c.find({"grp": 7}, sort=[("num", 1)], limit=10),
        **medium,
    )
    run(
        "deep skip in indexed grp",
        lambda: c.find({"grp": 7}, skip=500, limit=10),
        **medium,
    )
    run(
        "projection include 3 fields",
        lambda: c.find(
            {"grp": 7}, projection={"grp": 1, "num": 1, "str": 1, "_id": 0}, limit=10
        ),
        **fast,
    )

    print("\nCOUNT")
    print("-" * 122)
    run("count all", lambda: c.count({}), **fast)
    run("count indexed grp", lambda: c.count({"grp": 7}), **fast)
    run(
        "count indexed num range",
        lambda: c.count({"num": {"$gte": 100_000, "$lt": 100_100}}),
        **fast,
    )
    run(
        "count unindexed tags $all",
        lambda: c.count({"tags": {"$all": ["alpha", "beta"]}}),
        **scan,
    )


def bench_aggregate(db: NonoDB) -> None:
    c = db.collection("opsbench")
    print("\nAGGREGATION")
    print("-" * 122)
    match_100k = {"_id": {"$lt": "d00100000"}}
    match_30k = {"_id": {"$lt": "d00030000"}}
    iters = dict(iters=10, warmup=2)
    scan = dict(iters=5, warmup=1)

    run(
        "$match _id range + $count 100k",
        lambda: c.aggregate([{"$match": match_100k}, {"$count": "rows"}]),
        **iters,
    )
    run(
        "$match indexed grp=7 + $count",
        lambda: c.aggregate([{"$match": {"grp": 7}}, {"$count": "rows"}]),
        **iters,
    )
    run(
        "$project include + $count 100k",
        lambda: c.aggregate(
            [
                {"$match": match_100k},
                {"$project": {"_id": 0, "seq": 1, "grp": 1, "num": 1}},
                {"$count": "rows"},
            ]
        ),
        **iters,
    )
    run(
        "$project computed + $count 100k",
        lambda: c.aggregate(
            [
                {"$match": match_100k},
                {
                    "$project": {
                        "_id": 0,
                        "seq": 1,
                        "double": {"$multiply": ["$num", 2]},
                        "sum": {"$add": ["$seq", 3]},
                        "label": {"$concat": ["$tenant", "-", "$str"]},
                        "kind": {"$cond": [{"$gte": ["$seq", 50_000]}, "high", "low"]},
                        "fallback": {"$ifNull": ["$opt", -1]},
                    }
                },
                {"$count": "rows"},
            ]
        ),
        **iters,
    )
    run(
        "$addFields + $count 100k",
        lambda: c.aggregate(
            [
                {"$match": match_100k},
                {"$addFields": {"bucket": {"$multiply": ["$grp", 10]}}},
                {"$count": "rows"},
            ]
        ),
        **iters,
    )
    run(
        "$unset + $count 100k",
        lambda: c.aggregate(
            [
                {"$match": match_100k},
                {"$unset": ["items", "scores", "tags"]},
                {"$count": "rows"},
            ]
        ),
        **iters,
    )
    run(
        "$sort + $limit 100k",
        lambda: c.aggregate(
            [{"$match": match_100k}, {"$sort": {"num": -1}}, {"$limit": 100}]
        ),
        **iters,
    )
    run(
        "$skip half + $limit 100",
        lambda: c.aggregate(
            [{"$match": match_100k}, {"$skip": 50_000}, {"$limit": 100}]
        ),
        **iters,
    )
    run("$limit 100", lambda: c.aggregate([{"$match": match_100k}, {"$limit": 100}]), **iters)
    run("$count", lambda: c.aggregate([{"$match": match_100k}, {"$count": "rows"}]), **iters)
    run(
        "$group sum/avg/min/max/count",
        lambda: c.aggregate(
            [
                {"$match": match_100k},
                {
                    "$group": {
                        "_id": "$grp",
                        "total": {"$sum": "$num"},
                        "avg": {"$avg": "$num"},
                        "min": {"$min": "$num"},
                        "max": {"$max": "$num"},
                        "first": {"$first": "$seq"},
                        "last": {"$last": "$seq"},
                        "rows": {"$count": {}},
                    }
                },
            ]
        ),
        **iters,
    )
    run(
        "$group decimal sum/avg",
        lambda: c.aggregate(
            [
                {"$match": match_100k},
                {
                    "$group": {
                        "_id": "$grp",
                        "total": {"$sum": "$dec"},
                        "avg": {"$avg": "$dec"},
                        "rows": {"$sum": 1},
                    }
                },
            ]
        ),
        **iters,
    )
    run(
        "$group large long min/max",
        lambda: c.aggregate(
            [
                {"$match": match_100k},
                {
                    "$group": {
                        "_id": "$grp",
                        "min": {"$min": "$i64"},
                        "max": {"$max": "$i64"},
                    }
                },
            ]
        ),
        **iters,
    )
    run(
        "date range match + group",
        lambda: c.aggregate(
            [
                {
                    "$match": {
                        "date": {
                            "$gte": {"$date": (DATE0 + timedelta(minutes=100_000)).isoformat()},
                            "$lt": {"$date": (DATE0 + timedelta(minutes=200_000)).isoformat()},
                        }
                    }
                },
                {"$group": {"_id": "$grp", "n": {"$sum": 1}}},
            ]
        ),
        **iters,
    )
    run(
        "decimal range match + count",
        lambda: c.aggregate(
            [
                {
                    "$match": {
                        "dec": {
                            "$gte": {"$numberDecimal": "1000.00"},
                            "$lt": {"$numberDecimal": "2000.00"},
                        }
                    }
                },
                {"$count": "rows"},
            ]
        ),
        **iters,
    )
    run(
        "$sort date desc + limit",
        lambda: c.aggregate(
            [{"$match": match_100k}, {"$sort": {"date": -1}}, {"$limit": 100}]
        ),
        **iters,
    )
    run(
        "$group push/addToSet",
        lambda: c.aggregate(
            [
                {"$match": match_100k},
                {
                    "$group": {
                        "_id": "$nested.label",
                        "ids": {"$push": "$seq"},
                        "groups": {"$addToSet": "$grp"},
                        "rows": {"$sum": 1},
                    }
                },
            ]
        ),
        **iters,
    )
    run(
        "$unwind tags 30k + $count",
        lambda: c.aggregate(
            [{"$match": match_30k}, {"$unwind": "$tags"}, {"$count": "rows"}]
        ),
        **scan,
    )
    run(
        "combined filter+group+sort+limit",
        lambda: c.aggregate(
            [
                {"$match": match_100k},
                {"$group": {"_id": "$tenant", "total": {"$sum": "$num"}, "n": {"$sum": 1}}},
                {"$sort": {"total": -1}},
                {"$limit": 10},
                {"$project": {"_id": 0, "tenant": "$_id", "total": 1, "n": 1}},
            ]
        ),
        **iters,
    )
    run(
        "indexed match + group",
        lambda: c.aggregate(
            [
                {"$match": {"grp": {"$lt": 10}}},
                {"$group": {"_id": "$tenant", "total": {"$sum": "$num"}, "n": {"$sum": 1}}},
            ]
        ),
        **iters,
    )


def bench_text(db: NonoDB) -> None:
    c = db.collection("ops_text")
    print("\nTEXT / FUZZY SEARCH")
    print("-" * 122)
    fast = dict(iters=100, warmup=5)
    medium = dict(iters=30, warmup=2)
    run("BM25 single term", lambda: c.search("body", "database", limit=10), **fast)
    run("BM25 two terms", lambda: c.search("body", "rust async", limit=10), **fast)
    run("BM25 phrase", lambda: c.search("body", '"storage engine"', limit=10), **fast)
    run(
        "BM25 with filter",
        lambda: c.search("body", "database", filter={"cat": "c1"}, limit=10),
        **medium,
    )
    run("fuzzy distance 1", lambda: c.fuzzy("title", "databse", max_distance=1, limit=10), **medium)
    run("fuzzy distance 2", lambda: c.fuzzy("title", "postgress", max_distance=2, limit=10), **medium)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=1_000_000)
    ap.add_argument("--text-docs", type=int, default=200_000)
    ap.add_argument("--chunk", type=int, default=500)
    ap.add_argument("--skip-text", action="store_true")
    ap.add_argument(
        "--reuse",
        action="store_true",
        help="reuse existing opsbench/ops_text collections (skip loading)",
    )
    args = ap.parse_args()

    db = NonoDB(SOCK)
    print(f"docs={args.docs:,} text_docs={args.text_docs:,} socket={SOCK}")
    print("=" * 122)
    if args.reuse:
        print("REUSE existing opsbench/ops_text collections")
    else:
        load_main(db, args.docs, args.chunk)
        if not args.skip_text:
            load_text(db, args.text_docs, args.chunk)

    bench_find(db.collection("opsbench"))
    bench_aggregate(db)
    if not args.skip_text:
        bench_text(db)

    print("\nNote: daemon cache should be disabled (`NONODB_CACHE_TTL=0`) for")
    print("      repeatable execution timings. Numbers include UDS/HTTP and JSON.")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
