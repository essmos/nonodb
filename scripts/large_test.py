"""Large-data tests: loading, indexes, queries, update/delete, text.

    NONODB_SOCK=/tmp/nonodb.sock NONODB_PID=1234 \
        python3 scripts/large_test.py --docs 1000000 --text-docs 100000

Requires a running daemon and a fresh data directory (the script clears collections).
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
DATA = os.environ.get("NONODB_DATA", "")
PID = os.environ.get("NONODB_PID", "")

CITIES = [f"city{i:02d}" for i in range(50)]
WORDS = (
    "database rust python postgresql mysql mongodb document index search "
    "query storage memory async runtime web framework config user account "
    "order payment cache network security token service".split()
)


def pct(lat: list[float], q: float) -> float:
    s = sorted(lat)
    return s[min(len(s) - 1, int(q * (len(s) - 1)))] * 1000.0


def bench(label: str, fn, n: int) -> None:
    lat = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        lat.append(time.perf_counter() - t)
    print(
        f"  {label:<46} p50={pct(lat, .50):8.2f}ms  "
        f"p95={pct(lat, .95):8.2f}ms  ({n} prób)"
    )


def resources() -> str:
    parts = []
    if DATA:
        database = os.environ.get("NONODB_DB", "default")
        candidates = [
            os.path.join(DATA, database, "data.mdb"),
            os.path.join(DATA, "default", "data.mdb"),
            os.path.join(DATA, "data.mdb"),
        ]
        f = next((path for path in candidates if os.path.isfile(path)), "")
        if f:
            parts.append(f"data.mdb={os.path.getsize(f) / 1e6:.1f} MB")
    if PID and os.path.exists(f"/proc/{PID}/status"):
        with open(f"/proc/{PID}/status") as fh:
            for line in fh:
                if line.startswith(("VmRSS:", "VmSize:")):
                    parts.append(line.replace(":", "=").strip())
    return "  ".join(parts)


def make_doc(i: int) -> dict:
    return {
        "_id": f"u{i:08d}",
        "grp": i % 1000,
        "age": i % 90,
        "score": i % 1000,
        "city": CITIES[i % 50],
        "name": f"user {i}",
        "payload": "x" * 100,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=1_000_000)
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--text-docs", type=int, default=100_000)
    ap.add_argument("--text", action="store_true", help="także BM25/fuzzy")
    ap.add_argument("--scan-samples", type=int, default=20)
    args = ap.parse_args()

    db = NonoDB(SOCK, timeout=float(os.environ.get("NONODB_TIMEOUT", "10")))
    c = db.collection("large")

    # --- loading ----------------------------------------------------
    t = time.perf_counter()
    if c.count({}) > 0:
        print("kolekcja `large` nie jest pusta — czyszczę (może potrwać)...")
        c.delete({}, multi=True)
    for j in range(0, args.docs, args.batch):
        c.insert_many(
            [make_doc(i) for i in range(j, min(j + args.batch, args.docs))]
        )
    load = time.perf_counter() - t
    print(
        f"LOAD  {args.docs} dok. w {load:.1f}s -> {args.docs / load:,.0f} dok/s"
    )
    print("  ", resources())

    # --- indexes -----------------------------------------------------
    t = time.perf_counter()
    c.create_index("grp")
    c.create_index("age")
    c.create_index("score")
    print(f"INDEX 3 pola (backfill) w {time.perf_counter() - t:.1f}s")
    print("  ", resources())

    # --- queries -----------------------------------------------------
    rnd = random.Random(42)
    ids = [f"u{rnd.randrange(args.docs):08d}" for _ in range(1000)]
    print("QUERY")

    bench("find_one po _id (direct get)", lambda: c.find_one({"_id": rnd.choice(ids)}), 300)
    bench(
        "find(grp=x, limit=10) — indeks",
        lambda: c.find({"grp": rnd.randrange(1000)}, limit=10),
        200,
    )
    bench(
        "find(age in [18,30), limit=10) — zakres",
        lambda: c.find({"age": {"$gte": 18, "$lt": 30}}, limit=10),
        200,
    )
    bench("count({}) — O(1)", lambda: c.count({}), 500)
    bench(
        "count(grp=x) — indeks",
        lambda: c.count({"grp": rnd.randrange(1000)}),
        200,
    )
    bench(
        "find(grp=x, skip=500, limit=10) — głęboki skip",
        lambda: c.find({"grp": rnd.randrange(1000)}, skip=500, limit=10),
        50,
    )
    bench(
        "find(grp=x, sort age, limit=10)",
        lambda: c.find({"grp": rnd.randrange(1000)}, sort=[("age", 1)], limit=10),
        50,
    )
    bench(
        "find(name=..., limit=1) — bez indeksu",
        lambda: c.find_one({"name": f"user {rnd.randrange(args.docs)}"}),
        args.scan_samples,
    )

    # batch
    t = time.perf_counter()
    for j in range(0, len(ids), 100):
        db.batch(
            [
                {"op": "find_one", "coll": "large", "filter": {"_id": i}}
                for i in ids[j : j + 100]
            ]
        )
    b = time.perf_counter() - t
    print(f"  {'1000x find_one — batch (10×100)':<46} {b * 1000:8.2f}ms  ({len(ids) / b:,.0f} ops/s)")

    # --- update/delete ----------------------------------------------
    t = time.perf_counter()
    n = c.update({"grp": 7}, {"$set": {"flag": True}}, multi=True)
    print(f"UPDATE grp=7: {n} dok. w {time.perf_counter() - t:.2f}s")
    assert c.count({"grp": 7, "flag": True}) == n

    t = time.perf_counter()
    n = c.delete({"grp": 8}, multi=True)
    print(f"DELETE grp=8: {n} dok. w {time.perf_counter() - t:.2f}s")
    assert c.count({}) == args.docs - n
    print("  ", resources())

    # --- text / fuzzy -----------------------------------------------
    if args.text:
        t = db.collection("large_text")
        t.delete({}, multi=True)
        t.drop_text_index("body")
        t.drop_fuzzy_index("title")
        t0 = time.perf_counter()
        for j in range(0, args.text_docs, args.batch):
            docs = []
            for i in range(j, min(j + args.batch, args.text_docs)):
                docs.append(
                    {
                        "_id": f"t{i:08d}",
                        "title": f"{rnd.choice(WORDS).title()} {rnd.choice(WORDS)} {i}",
                        "body": " ".join(rnd.choice(WORDS) for _ in range(12)),
                        "grp": i % 100,
                    }
                )
            t.insert_many(docs)
        print(
            f"TEXT load {args.text_docs} dok. w {time.perf_counter() - t0:.1f}s"
        )
        t0 = time.perf_counter()
        t.create_text_index("body")
        print(f"TEXT index (backfill) w {time.perf_counter() - t0:.1f}s")
        t0 = time.perf_counter()
        t.create_fuzzy_index("title")
        print(f"FUZZY index (backfill) w {time.perf_counter() - t0:.1f}s")
        qs = ["database", "rust async", "postgresql query", "storage memory"]
        bench("search BM25 (limit=10)", lambda: t.search("body", rnd.choice(qs)), 100)
        fq = ["databse", "postgress", "pythn", "rustt"]
        bench(
            "fuzzy (max_distance=2)",
            lambda: t.fuzzy("title", rnd.choice(fq), limit=10),
            50,
        )
        print("  ", resources())

    print("count_total:", c.count({}))
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
