"""Load and benchmark the same MS MARCO subset in Elasticsearch.

This is a comparison harness, not a NonoDB dependency. It expects an
Elasticsearch HTTP endpoint (for example a local single-node container).

    python3 scripts/es_bench.py --url http://127.0.0.1:9200 \
      --collection-tsv collection.tsv --queries queries.train.tsv \
      --load --samples 200 --clients 16

The load phase creates a one-shard, zero-replica index with refresh disabled,
bulk-loads the corpus, then refreshes once. The query phase uses `match` with
`operator=and`, `size=10`, and `track_total_hits=false` to match NonoDB's
single-field AND search as closely as possible.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests


def load(args: argparse.Namespace) -> None:
    session = requests.Session()
    session.delete(f"{args.url}/{args.index}")
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "-1",
        },
        "mappings": {"properties": {"body": {"type": "text"}}},
    }
    session.put(f"{args.url}/{args.index}", json=mapping).raise_for_status()
    started = time.perf_counter()
    docs = 0
    batch: list[str] = []
    with open(args.collection_tsv, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if args.limit and docs >= args.limit:
                break
            doc_id, sep, body = line.rstrip("\n").partition("\t")
            if not sep:
                continue
            batch.append(json.dumps({"index": {"_index": args.index, "_id": doc_id}}))
            batch.append(json.dumps({"body": body}))
            docs += 1
            if len(batch) >= args.batch * 2:
                response = session.post(
                    f"{args.url}/_bulk",
                    data=("\n".join(batch) + "\n").encode(),
                    headers={"Content-Type": "application/x-ndjson"},
                    timeout=args.timeout,
                )
                response.raise_for_status()
                if response.json().get("errors"):
                    raise SystemExit(response.text[:1000])
                batch.clear()
                if docs % 100_000 == 0:
                    print(
                        f"load {docs:,} {time.perf_counter() - started:.1f}s",
                        flush=True,
                    )
    if batch:
        response = session.post(
            f"{args.url}/_bulk",
            data=("\n".join(batch) + "\n").encode(),
            headers={"Content-Type": "application/x-ndjson"},
            timeout=args.timeout,
        )
        response.raise_for_status()
        if response.json().get("errors"):
            raise SystemExit(response.text[:1000])
    session.post(f"{args.url}/{args.index}/_refresh", timeout=args.timeout).raise_for_status()
    print(
        f"loaded {docs:,} in {time.perf_counter() - started:.1f}s",
        flush=True,
    )


def load_queries(path: str, samples: int) -> list[str]:
    queries = []
    with open(path, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            _qid, sep, query = line.rstrip("\n").partition("\t")
            if sep and query.strip():
                queries.append(query.strip())
    rng = random.Random(42)
    if samples < len(queries):
        queries = rng.sample(queries, samples)
    return queries


def pct(values: list[float], q: float) -> float:
    return sorted(values)[min(len(values) - 1, int(q * (len(values) - 1)))] * 1000.0


def bench(args: argparse.Namespace) -> None:
    queries = load_queries(args.queries, args.samples)
    if not queries:
        raise SystemExit("no queries found")
    template = {
        "size": args.size,
        "track_total_hits": False,
        "query": {"match": {"body": {"query": None, "operator": "and"}}},
    }

    def query(session: requests.Session, text: str) -> float:
        body = json.loads(json.dumps(template))
        body["query"]["match"]["body"]["query"] = text
        started = time.perf_counter()
        response = session.post(
            f"{args.url}/{args.index}/_search", json=body, timeout=args.timeout
        )
        response.raise_for_status()
        return time.perf_counter() - started

    warm = requests.Session()
    for text in queries[: args.warmup]:
        query(warm, text)
    warm.close()

    def worker(_: int) -> list[float]:
        session = requests.Session()
        try:
            return [query(session, text) for text in queries]
        finally:
            session.close()

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.clients) as pool:
        chunks = list(pool.map(worker, range(args.clients)))
    wall = time.perf_counter() - started
    latencies = [value for chunk in chunks for value in chunk]
    print(
        f"ES clients={args.clients} total={len(latencies)} "
        f"p50={pct(latencies, 0.50):.2f}ms "
        f"p95={pct(latencies, 0.95):.2f}ms "
        f"p99={pct(latencies, 0.99):.2f}ms "
        f"qps={len(latencies) / wall:.1f}",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:9200")
    parser.add_argument("--index", default="msmarco1m")
    parser.add_argument("--collection-tsv", required=True)
    parser.add_argument("--queries", required=True)
    parser.add_argument("--limit", type=int, default=1_000_000)
    parser.add_argument("--batch", type=int, default=5_000)
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--size", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=1200)
    parser.add_argument("--load", action="store_true")
    args = parser.parse_args()
    if args.load:
        load(args)
    bench(args)


if __name__ == "__main__":
    main()
