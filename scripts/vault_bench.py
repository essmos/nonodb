"""Vault encryption benchmark: standard collection vs vault collection.

Run against a daemon (preferably with cache disabled for standard comparisons):

    NONODB_CACHE_TTL=0 NONODB_SOCK=/tmp/nonodb.sock \
        python3 scripts/vault_bench.py --docs 5000 --iters 1000

The benchmark compares the same operations on a standard collection and a
vault collection: bulk insert, read by `_id`, list by tenant, count, and update
by `_id`. Vault operations include encryption/decryption; standard operations
are measured on the same daemon and payload size.
"""

from __future__ import annotations

import argparse
import base64
import os
import random
import statistics
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
TENANT = "bench-tenant"
KID = "bench:v1"


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(round(q * (len(values) - 1))))] * 1000.0


def summarize(label: str, lat: list[float], elapsed: float, n: int) -> None:
    rate = n / elapsed if elapsed > 0 else 0.0
    print(
        f"{label:<34} {n:>7} ops  {rate:>8.0f} ops/s"
        f"  p50={pct(lat, 0.50):7.2f} ms"
        f"  p95={pct(lat, 0.95):7.2f} ms"
        f"  p99={pct(lat, 0.99):7.2f} ms"
    )


def bench_calls(label: str, calls: int, fn) -> float:
    lat: list[float] = []
    start = time.perf_counter()
    for _ in range(calls):
        t = time.perf_counter()
        fn()
        lat.append(time.perf_counter() - t)
    elapsed = time.perf_counter() - start
    summarize(label, lat, elapsed, calls)
    return statistics.median(lat) * 1000.0


def reset_standard(db: NonoDB, name: str) -> None:
    db.create_collection(name, "standard")
    db.collection(name).delete({}, multi=True)


def reset_vault(db: NonoDB, name: str, vault) -> None:
    db.create_collection(name, "vault")
    c = vault.collection(name)
    while True:
        docs = c.find({"tenant_id": TENANT}, limit=1000)
        if not docs:
            break
        for doc in docs:
            c.delete({"_id": doc["_id"]})


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=5000)
    ap.add_argument("--iters", type=int, default=1000)
    ap.add_argument("--chunk", type=int, default=200)
    ap.add_argument("--payload", type=int, default=256)
    args = ap.parse_args()
    if args.docs <= 0 or args.iters <= 0:
        print("--docs and --iters must be positive")
        return 1

    key = base64.b64encode(os.urandom(32)).decode()
    db = NonoDB(SOCK, timeout=120)
    vault = db.vault(TENANT, KID, key)
    std_name = "vault_bench_std"
    vault_name = "vault_bench_vault"

    reset_standard(db, std_name)
    reset_vault(db, vault_name, vault)
    std = db.collection(std_name)
    vc = vault.collection(vault_name)

    payload = "x" * args.payload
    docs = [
        {
            "tenant_id": TENANT,
            "seq": i,
            "grp": i % 100,
            "payload": payload,
        }
        for i in range(args.docs)
    ]

    print(
        f"docs={args.docs} iters={args.iters} chunk={args.chunk} "
        f"payload={args.payload}B cache=NONODB_CACHE_TTL=0 (recommended)"
    )

    # Bulk insert: same chunk size and documents for both profiles.
    t = time.perf_counter()
    for i in range(0, len(docs), args.chunk):
        std.insert_many(docs[i : i + args.chunk])
    std_insert = time.perf_counter() - t
    print(
        f"insert_many standard             {args.docs:>7} docs"
        f"  {args.docs / std_insert:>8.0f} docs/s"
        f"  total={std_insert:6.2f}s"
    )

    t = time.perf_counter()
    for i in range(0, len(docs), args.chunk):
        vc.insert_many(docs[i : i + args.chunk])
    vault_insert = time.perf_counter() - t
    print(
        f"insert_many vault                {args.docs:>7} docs"
        f"  {args.docs / vault_insert:>8.0f} docs/s"
        f"  total={vault_insert:6.2f}s"
        f"  overhead={vault_insert / std_insert:.2f}x"
    )

    std_ids = [d["_id"] for d in std.find({}, projection={"_id": 1}, limit=args.docs)]
    vault_ids = [d["_id"] for d in vc.find({"tenant_id": TENANT}, limit=args.docs)]
    if len(std_ids) != args.docs or len(vault_ids) != args.docs:
        print(f"not enough documents: std={len(std_ids)} vault={len(vault_ids)}")
        return 1

    rnd = random.Random(7)
    reads = [rnd.choice(std_ids) for _ in range(args.iters)]
    vreads = [rnd.choice(vault_ids) for _ in range(args.iters)]

    # Warm up connections / page cache.
    for i in range(min(50, args.iters)):
        std.find_one({"_id": reads[i]})
        vc.find_one({"_id": vreads[i]})

    std_read = bench_calls(
        "find_one by _id standard", args.iters, lambda: std.find_one({"_id": rnd.choice(reads)})
    )
    vault_read = bench_calls(
        "find_one by _id vault", args.iters, lambda: vc.find_one({"_id": rnd.choice(vreads)})
    )
    print(f"{'read p50 overhead':<34} {vault_read / std_read:.2f}x")

    bench_calls(
        "find by tenant limit=100 standard",
        args.iters // 5 or 1,
        lambda: std.find({"tenant_id": TENANT}, limit=100),
    )
    bench_calls(
        "find by tenant limit=100 vault",
        args.iters // 5 or 1,
        lambda: vc.find({"tenant_id": TENANT}, limit=100),
    )

    bench_calls(
        "count by tenant standard", args.iters // 5 or 1, lambda: std.count({"tenant_id": TENANT})
    )
    bench_calls(
        "count by tenant vault", args.iters // 5 or 1, lambda: vc.count({"tenant_id": TENANT})
    )

    upd_ids = [rnd.choice(std_ids) for _ in range(args.iters)]
    vupd_ids = [rnd.choice(vault_ids) for _ in range(args.iters)]
    std_update = bench_calls(
        "update by _id standard",
        args.iters,
        lambda: std.update({"_id": rnd.choice(upd_ids)}, {"$inc": {"n": 1}}),
    )
    vault_update = bench_calls(
        "update by _id vault",
        args.iters,
        lambda: vc.update({"_id": rnd.choice(vupd_ids)}, {"$inc": {"n": 1}}),
    )
    print(f"{'update p50 overhead':<34} {vault_update / std_update:.2f}x")

    print("vault count:", vc.count({"tenant_id": TENANT}))
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
