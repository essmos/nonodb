"""Compute exact cosine top-10 neighbours for the prepared query set.

The result is the ground truth for recall@10 and for verifying every ANN engine.
Input vectors must already be L2-normalized (see prepare_msmarco.py).

Writes:
    ground_truth.npy   int32 [queries, 10] row indices into embeddings.npy
    ground_truth.json  metadata

Example:
    python ground_truth.py --data /data/vector-compare/500k --top 10
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, help="prepared data directory")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--batch", type=int, default=32)
    parser.add_argument("--output", default="ground_truth.npy")
    args = parser.parse_args()

    data = Path(args.data)
    embeddings = np.load(data / "embeddings.npy", mmap_mode="r")
    queries = np.load(data / "query_embeddings.npy", mmap_mode="r")
    passage_vectors = np.asarray(embeddings)
    if embeddings.shape[1] != queries.shape[1]:
        raise SystemExit("passage and query dimensions differ")

    top = args.top
    truth = np.empty((queries.shape[0], top), dtype=np.int32)
    started = time.perf_counter()
    for start in range(0, queries.shape[0], args.batch):
        end = min(start + args.batch, queries.shape[0])
        # Both inputs are normalized, so dot product is cosine similarity.
        scores = np.asarray(queries[start:end]) @ passage_vectors.T
        candidates = np.argpartition(-scores, top, axis=1)[:, :top]
        rows = np.arange(end - start)[:, None]
        candidate_scores = scores[rows, candidates]
        order = np.lexsort((candidates, -candidate_scores), axis=1)
        truth[start:end] = candidates[rows, order]
        print(f"TRUTH {end:,}/{queries.shape[0]:,}", flush=True)

    np.save(data / args.output, truth)
    meta = {
        "queries": int(queries.shape[0]),
        "passages": int(embeddings.shape[0]),
        "dimensions": int(embeddings.shape[1]),
        "top": top,
        "metric": "cosine",
        "rows": "indices into embeddings.npy and pass_ids.txt",
        "seconds": round(time.perf_counter() - started, 2),
    }
    (data / "ground_truth.json").write_text(
        json.dumps(meta, indent=2) + "\n", encoding="utf-8"
    )
    print(f"DONE {json.dumps(meta)}", flush=True)


if __name__ == "__main__":
    main()
