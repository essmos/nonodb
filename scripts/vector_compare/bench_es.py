"""Load and benchmark the prepared vectors in Elasticsearch (dense_vector/HNSW)."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np

from bench_common import benchmark, load_data


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--url", default="http://127.0.0.1:9200")
    parser.add_argument("--index", default="vector-compare")
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--label", default="elasticsearch")
    parser.add_argument("--hnsw-m", type=int, default=16)
    parser.add_argument("--ef-construction", type=int, default=100)
    parser.add_argument("--num-candidates", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--rounds", type=int, default=0)
    parser.add_argument("--output", default="results")
    args = parser.parse_args()

    from elasticsearch import Elasticsearch, NotFoundError
    from elasticsearch.helpers import bulk

    data = Path(args.data)
    meta = json.loads((data / "meta.json").read_text(encoding="utf-8"))
    dimensions = int(meta["dimensions"])
    client = Elasticsearch(args.url, request_timeout=1800)
    index_seconds = None

    def exists() -> bool:
        try:
            return bool(client.indices.exists(index=args.index))
        except NotFoundError:
            return False

    if args.recreate and exists():
        client.indices.delete(index=args.index)
    created = exists()

    if args.load:
        if not created:
            client.indices.create(
                index=args.index,
                settings={"number_of_shards": 1, "number_of_replicas": 0},
                mappings={
                    "properties": {
                        "body": {"type": "text"},
                        "vec": {
                            "type": "dense_vector",
                            "dims": dimensions,
                            "index": True,
                            "similarity": "cosine",
                            "index_options": {
                                "type": "hnsw",
                                "m": args.hnsw_m,
                                "ef_construction": args.ef_construction,
                            },
                        },
                    }
                },
            )
            created = True
        embeddings = np.load(data / "embeddings.npy", mmap_mode="r")
        started = time.perf_counter()

        def actions():
            with (data / "passages.jsonl").open("r", encoding="utf-8") as handle:
                for row, line in enumerate(handle):
                    passage = json.loads(line)
                    yield {
                        "_index": args.index,
                        "_id": str(row),
                        "_source": {
                            "body": passage["body"],
                            "vec": embeddings[row].tolist(),
                        },
                    }

        loaded, errors = bulk(
            client.options(request_timeout=1800),
            actions(),
            chunk_size=args.batch,
            raise_on_error=False,
        )
        if errors:
            raise SystemExit(f"bulk errors: {errors[:3]}")
        client.indices.refresh(index=args.index)
        client.indices.forcemerge(index=args.index, max_num_segments=1)
        index_seconds = time.perf_counter() - started
        count = client.count(index=args.index)["count"]
        print(
            f"LOAD_DONE {loaded:,} count={count:,} in {index_seconds:.1f}s "
            f"{loaded / index_seconds:,.0f}/s",
            flush=True,
        )

    if args.benchmark:
        queries, truth, _ = load_data(data)

        def search(vector: list[float], limit: int) -> list[int]:
            response = client.search(
                index=args.index,
                body={
                    "knn": {
                        "field": "vec",
                        "query_vector": vector,
                        "k": limit,
                        "num_candidates": args.num_candidates,
                    },
                    "size": limit,
                    "_source": False,
                },
            )
            return [int(hit["_id"]) for hit in response["hits"]["hits"]]

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
                "index": args.index,
                "hnsw_m": args.hnsw_m,
                "ef_construction": args.ef_construction,
                "num_candidates": args.num_candidates,
                "index_seconds": None if index_seconds is None else round(index_seconds, 1),
                "data": str(data),
                "passages": meta["passages"],
            },
        )
    client.close()


if __name__ == "__main__":
    main()
