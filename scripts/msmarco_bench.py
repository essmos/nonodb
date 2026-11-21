"""Benchmark real MS MARCO queries against a loaded NonoDB collection.

    NONODB_SOCK=/tmp/nonodb.sock \
      python3 scripts/msmarco_bench.py queries.train.tsv --samples 200

Each query is run through the native BM25 `search` operation with `limit=10`.
The default sample is deterministic so runs are comparable.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLIENT = os.environ.get("NONODB_CLIENT") or os.path.join(ROOT, "client")
sys.path.insert(0, CLIENT)

from nonodb import NonoDB  # noqa: E402


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * (len(ordered) - 1)))] * 1000.0


def load_queries(path: str) -> list[str]:
    queries = []
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            _qid, sep, query = line.rstrip("\n").partition("\t")
            if sep and query.strip():
                queries.append(query.strip())
    return queries


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("queries", help="MS MARCO queries TSV (qid \\t query)")
    parser.add_argument("--collection", default="msmarco")
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--query-timeout", type=float, default=1800)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--clients", type=int, default=1)
    args = parser.parse_args()

    queries = load_queries(args.queries)
    if not queries:
        raise SystemExit("no queries found")
    rng = random.Random(args.seed)
    if args.samples < len(queries):
        queries = rng.sample(queries, args.samples)

    sock = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
    clients = max(1, args.clients)
    results: list[tuple[float, str] | None] = [None] * clients
    errors: list[BaseException] = []

    def worker(index: int) -> None:
        try:
            db = NonoDB(sock, timeout=args.query_timeout)
            c = db.collection(args.collection)
            for query in queries[: args.warmup]:
                c.search("body", query, limit=args.limit)
            local: list[tuple[float, str]] = []
            for query in queries:
                begin = time.perf_counter()
                c.search("body", query, limit=args.limit)
                local.append((time.perf_counter() - begin, query))
            db.close()
            results[index] = local  # type: ignore[assignment]
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    started = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(clients)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    wall = time.perf_counter() - started
    if errors:
        raise errors[0]

    latencies: list[float] = []
    slowest: list[tuple[float, str]] = []
    for result in results:
        if result is None:
            continue
        for elapsed, query in result:
            latencies.append(elapsed)
            slowest.append((elapsed, query))
    if not latencies:
        raise SystemExit("no results")
    slowest.sort(reverse=True)
    print(
        f"collection={args.collection} clients={clients} "
        f"queries_per_client={len(queries)} total={len(latencies)} limit={args.limit}",
        flush=True,
    )
    print(
        f"p50={pct(latencies, 0.50):.2f}ms "
        f"p95={pct(latencies, 0.95):.2f}ms "
        f"p99={pct(latencies, 0.99):.2f}ms "
        f"mean={sum(latencies) / len(latencies) * 1000:.2f}ms "
        f"qps={len(latencies) / wall:.1f}",
        flush=True,
    )
    print("slowest:", flush=True)
    for elapsed, query in slowest[:5]:
        print(f"  {elapsed * 1000:9.2f}ms  {query}", flush=True)


if __name__ == "__main__":
    main()
