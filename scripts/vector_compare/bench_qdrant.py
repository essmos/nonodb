"""Load and benchmark the prepared vectors in Qdrant (HNSW)."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np

from bench_common import benchmark, load_data


def wait_ready(client, collection: str, timeout: float) -> float:
    started = time.perf_counter()
    while True:
        info = client.get_collection(collection)
        status = str(info.status)
        optimizer = str(info.optimizer_status)
        if status.lower().endswith("green") and "ok" in optimizer.lower():
            return time.perf_counter() - started
        if time.perf_counter() - started > timeout:
            raise SystemExit(f"Qdrant collection not ready: {status} {optimizer}")
        print(f"WAIT qdrant status={status} optimizer={optimizer}", flush=True)
        time.sleep(5)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--url", default="http://127.0.0.1:6333")
    parser.add_argument("--collection", default="vector_compare")
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--label", default="qdrant")
    parser.add_argument("--hnsw-m", type=int, default=16)
    parser.add_argument("--ef-construct", type=int, default=100)
    parser.add_argument("--hnsw-ef", type=int, default=64)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--rounds", type=int, default=0)
    parser.add_argument("--output", default="results")
    parser.add_argument("--ready-timeout", type=float, default=1800)
    args = parser.parse_args()

    from qdrant_client import QdrantClient, models

    data = Path(args.data)
    meta = json.loads((data / "meta.json").read_text(encoding="utf-8"))
    dimensions = int(meta["dimensions"])
    client = QdrantClient(url=args.url, timeout=120)
    exists = args.collection in {item.name for item in client.get_collections().collections}

    if args.recreate and exists:
        client.delete_collection(args.collection)
        exists = False

    if args.load:
        if not exists:
            client.create_collection(
                collection_name=args.collection,
                vectors_config=models.VectorParams(
                    size=dimensions,
                    distance=models.Distance.COSINE,
                ),
                hnsw_config=models.HnswConfigDiff(
                    m=args.hnsw_m,
                    ef_construct=args.ef_construct,
                ),
            )
            exists = True
        embeddings = np.load(data / "embeddings.npy", mmap_mode="r")
        started = time.perf_counter()
        loaded = 0
        for start in range(0, embeddings.shape[0], args.batch):
            end = min(start + args.batch, embeddings.shape[0])
            points = [
                models.PointStruct(id=row, vector=embeddings[row].tolist())
                for row in range(start, end)
            ]
            client.upsert(collection_name=args.collection, points=points, wait=False)
            loaded = end
            if loaded % 50_000 < args.batch:
                elapsed = time.perf_counter() - started
                print(f"LOAD {loaded:,} {loaded / elapsed:,.0f}/s", flush=True)
        elapsed = time.perf_counter() - started
        print(f"LOAD_DONE {loaded:,} in {elapsed:.1f}s {loaded / elapsed:,.0f}/s", flush=True)
        ready = wait_ready(client, args.collection, args.ready_timeout)
        print(f"INDEX_READY in {ready:.1f}s after upsert completion", flush=True)

    if args.benchmark:
        queries, truth, _ = load_data(data)
        search_params = models.SearchParams(hnsw_ef=args.hnsw_ef)

        def search(vector: list[float], limit: int) -> list[int]:
            response = client.query_points(
                collection_name=args.collection,
                query=vector,
                limit=limit,
                search_params=search_params,
                with_payload=False,
            )
            return [int(point.id) for point in response.points]

        rounds = args.rounds or len(queries)
        benchmark(
            args.label,
            search,
            queries,
            truth,
            args.warmup,
            rounds,
            Path(args.output) / f"{args.label}.json",
            {
                "url": args.url,
                "collection": args.collection,
                "hnsw_m": args.hnsw_m,
                "ef_construct": args.ef_construct,
                "hnsw_ef": args.hnsw_ef,
                "data": str(data),
                "passages": meta["passages"],
            },
        )
    client.close()


if __name__ == "__main__":
    main()
