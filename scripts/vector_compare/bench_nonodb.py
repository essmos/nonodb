"""Load and benchmark the prepared MS MARCO vectors in NonoDB.

Use ``--load`` once, then restart the daemon with
``NONODB_BUILD_VECTOR_ANN=1 NONODB_VECTOR_ANN_COLLECTIONS=<collection>`` and run
``--benchmark`` again for the ANN path. Without a sidecar the same query uses
the exact LMDB vector index, which is useful as a baseline.

Example:
    python bench_nonodb.py --data /data/vector-compare/500k \
      --socket /run/nonodb.sock --load --create-index
    python bench_nonodb.py --data /data/vector-compare/500k \
      --socket /run/nonodb.sock --benchmark --label exact
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import numpy as np

from bench_common import benchmark, load_data


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--socket", default=os.environ.get("NONODB_SOCK", "/run/nonodb.sock"))
    parser.add_argument("--client", default=os.environ.get("NONODB_CLIENT", ""))
    parser.add_argument("--collection", default="vector_compare")
    parser.add_argument("--batch", type=int, default=200)
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--create-index", action="store_true")
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--label", default="nonodb")
    parser.add_argument("--nprobe", type=int, default=None, help="effective ANN probe override")
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--rounds", type=int, default=0, help="0 = all queries")
    parser.add_argument("--output", default="results")
    args = parser.parse_args()

    client_path = Path(args.client) if args.client else Path(__file__).resolve().parents[2] / "client"
    sys.path.insert(0, str(client_path))
    from nonodb import NonoDB  # noqa: E402

    data = Path(args.data)
    meta = json.loads((data / "meta.json").read_text(encoding="utf-8"))
    db = NonoDB(args.socket, timeout=1800)
    c = db.collection(args.collection)

    if args.recreate:
        for index in c.list_vector_indexes():
            c.drop_vector_index(index["field"])
        c.delete({}, multi=True)
        print("RECREATED collection", flush=True)

    if args.load:
        embeddings = np.load(data / "embeddings.npy", mmap_mode="r")
        started = time.perf_counter()
        loaded = 0
        batch: list[dict] = []
        with (data / "passages.jsonl").open("r", encoding="utf-8") as handle:
            for row, line in enumerate(handle):
                passage = json.loads(line)
                batch.append(
                    {
                        "_id": passage["_id"],
                        "body": passage["body"],
                        "vec": embeddings[row].tolist(),
                    }
                )
                if len(batch) >= args.batch:
                    c.insert_many(batch, durability="relaxed")
                    loaded += len(batch)
                    batch = []
                    if loaded % 50_000 < args.batch:
                        elapsed = time.perf_counter() - started
                        print(f"LOAD {loaded:,} {loaded / elapsed:,.0f}/s", flush=True)
        if batch:
            c.insert_many(batch, durability="relaxed")
            loaded += len(batch)
        elapsed = time.perf_counter() - started
        print(f"LOAD_DONE {loaded:,} in {elapsed:.1f}s {loaded / elapsed:,.0f}/s", flush=True)

    if args.create_index:
        definitions = {index["field"]: index for index in c.list_vector_indexes()}
        if "vec" in definitions:
            print("INDEX exists", json.dumps(definitions["vec"]), flush=True)
        else:
            started = time.perf_counter()
            c.create_vector_index("vec", int(meta["dimensions"]), "cosine")
            print(f"INDEX created in {time.perf_counter() - started:.1f}s", flush=True)

    if args.benchmark:
        queries, truth, _ = load_data(data)
        pass_ids = (data / "pass_ids.txt").read_text(encoding="utf-8").splitlines()
        row_of = {passage_id: row for row, passage_id in enumerate(pass_ids)}
        definitions = {index["field"]: index for index in c.list_vector_indexes()}

        def search(vector: list[float], limit: int) -> list[int]:
            hits = c.vector_search("vec", vector, limit=limit)
            return [row_of[str(hit["_id"])] for hit in hits if str(hit["_id"]) in row_of]

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
                "collection": args.collection,
                "vector_index": definitions.get("vec"),
                "nprobe": args.nprobe,
                "socket": args.socket,
                "data": str(data),
                "passages": meta["passages"],
            },
        )
    db.close()


if __name__ == "__main__":
    main()
