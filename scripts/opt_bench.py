"""Benchmark optymalizacji v1.x: count, find_one, update, group commit.

Uruchomienie (wymaga daemona):
    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/opt_bench.py --docs 50000
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
        f"{label:<42} p50={pct(lat, 0.50):7.2f}ms  p95={pct(lat, 0.95):7.2f}ms"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=50000)
    args = ap.parse_args()

    db = NonoDB(SOCK)
    c = db.collection("optbench")
    c.delete({}, multi=True)

    docs = [
        {
            "seq": i,
            "grp": i % 100,
            "payload": "x" * 50,
        }
        for i in range(args.docs)
    ]
    t = time.perf_counter()
    for j in range(0, len(docs), 500):
        c.insert_many(docs[j : j + 500])
    print(f"insert {args.docs} docs: {time.perf_counter() - t:.2f}s")
    c.create_index("grp")

    rnd = random.Random(7)

    # count: pełny skan (stare zachowanie) vs O(1) (nowe)
    bench("count(seq >= 0)  [pełny skan]", lambda: c.count({"seq": {"$gte": 0}}), 30)
    bench("count({})        [O(1) stat]", lambda: c.count({}), 500)

    # find_one: pełny skan (stare zachowanie) vs wczesne wyjście (nowe)
    seqs = [rnd.randrange(args.docs) for _ in range(20)]
    bench(
        "find(seq=...) bez limitu [pełny skan]",
        lambda: c.find({"seq": rnd.choice(seqs)}),
        20,
    )
    bench(
        "find_one(seq=...) [wczesne wyjście]",
        lambda: c.find_one({"seq": rnd.choice(seqs)}),
        300,
    )

    # indeks: przed zbierano wszystkie trafienia, teraz stop po 1
    grps = list(range(100))
    bench(
        "find_one(grp=...) [indeks + early exit]",
        lambda: c.find_one({"grp": rnd.choice(grps)}),
        300,
    )

    # update/delete przez indeks vs pełny skan
    bench(
        "update(grp=..., multi=true) [indeks]",
        lambda: c.update({"grp": rnd.choice(grps)}, {"$inc": {"n": 1}}, True),
        50,
    )
    bench(
        "update(seq=..., multi=false) [skan]",
        lambda: c.update(
            {"seq": rnd.choice(seqs)}, {"$inc": {"n": 1}}, False
        ),
        50,
    )

    # --- batch: N odczytów w jednym żądaniu -------------------------
    ids = [d["_id"] for d in c.find({}, projection={"_id": 1}, limit=1000)]
    db.batch([])  # rozgrzewka połączenia

    t = time.perf_counter()
    for i in ids:
        c.find_one({"_id": i})
    single = time.perf_counter() - t

    t = time.perf_counter()
    for j in range(0, len(ids), 100):
        db.batch(
            [
                {"op": "find_one", "coll": c.name, "filter": {"_id": i}}
                for i in ids[j : j + 100]
            ]
        )
    batched = time.perf_counter() - t
    print(
        f"1000x find_one: pojedynczo {single * 1000:7.1f}ms "
        f"({len(ids) / single:6.0f} ops/s) | batch {batched * 1000:7.1f}ms "
        f"({len(ids) / batched:6.0f} ops/s) | {single / batched:.1f}x"
    )

    print("count_total:", c.count({}))
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
