"""Prepare a real MS MARCO vector benchmark data set.

Reads ``collection.tsv`` and ``queries.train.tsv`` from the official MS MARCO
passage ranking archive, embeds the selected passages and queries with a
sentence-transformers model, and writes a self-contained directory:

    passages.jsonl          one {"_id", "body"} object per passage
    pass_ids.txt            passage ids, one per line (row order of embeddings)
    embeddings.npy          float32 [passages, dims]
    queries.jsonl           one {"_id", "text"} object per query
    query_ids.txt           query ids, one per line
    query_embeddings.npy    float32 [queries, dims]
    meta.json               model, dimensions, counts, and normalization

Vectors are L2-normalized, so cosine similarity is a dot product. The script
does not download the corpus or the model; point it at local files and allow
Hugging Face access for the model on first use.

Example:
    python prepare_msmarco.py \
      --collection /data/msmarco/collection.tsv \
      --queries /data/msmarco/queries.train.tsv \
      --out /data/vector-compare/500k \
      --limit 500000 --queries-limit 1000
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import numpy as np


def read_passages(path: Path, limit: int) -> tuple[list[str], list[str]]:
    ids: list[str] = []
    texts: list[str] = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if limit and len(ids) >= limit:
                break
            passage_id, separator, text = line.rstrip("\n").partition("\t")
            if not separator:
                continue
            ids.append(passage_id)
            texts.append(text)
    return ids, texts


def read_queries(path: Path, limit: int) -> tuple[list[str], list[str]]:
    ids: list[str] = []
    texts: list[str] = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if limit and len(ids) >= limit:
                break
            query_id, separator, text = line.rstrip("\n").partition("\t")
            if not separator or not text.strip():
                continue
            ids.append(query_id)
            texts.append(text.strip())
    return ids, texts


def embed(
    model,
    texts: list[str],
    dimensions: int,
    batch_size: int,
    chunk_size: int = 10_000,
) -> np.ndarray:
    vectors = np.empty((len(texts), dimensions), dtype=np.float32)
    started = time.perf_counter()
    for start in range(0, len(texts), chunk_size):
        end = min(start + chunk_size, len(texts))
        vectors[start:end] = model.encode(
            texts[start:end],
            batch_size=batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        done = end
        rate = done / max(time.perf_counter() - started, 1e-9)
        print(f"EMBED {done:,}/{len(texts):,} {rate:,.0f}/s", flush=True)
    return vectors


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--collection", required=True, help="MS MARCO collection.tsv")
    parser.add_argument("--queries", required=True, help="MS MARCO queries.train.tsv")
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument("--limit", type=int, default=500_000)
    parser.add_argument("--queries-limit", type=int, default=1_000)
    parser.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    parser.add_argument("--batch", type=int, default=128)
    parser.add_argument("--threads", type=int, default=8)
    args = parser.parse_args()

    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    import torch
    from sentence_transformers import SentenceTransformer

    torch.set_num_threads(max(1, args.threads))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    print(f"LOAD passages from {args.collection}", flush=True)
    pass_ids, pass_texts = read_passages(Path(args.collection), args.limit)
    print(f"LOAD {len(pass_ids):,} passages", flush=True)

    print(f"LOAD queries from {args.queries}", flush=True)
    query_ids, query_texts = read_queries(Path(args.queries), args.queries_limit)
    print(f"LOAD {len(query_ids):,} queries", flush=True)

    print(f"MODEL {args.model}", flush=True)
    started = time.perf_counter()
    model = SentenceTransformer(args.model, device="cpu")
    if hasattr(model, "get_embedding_dimension"):
        dimensions = model.get_embedding_dimension()
    else:
        dimensions = model.get_sentence_embedding_dimension()
    print(
        f"MODEL loaded in {time.perf_counter() - started:.1f}s dims={dimensions}",
        flush=True,
    )

    embeddings = embed(model, pass_texts, dimensions, args.batch)
    np.save(out / "embeddings.npy", embeddings)

    query_embeddings = embed(model, query_texts, dimensions, args.batch)
    np.save(out / "query_embeddings.npy", query_embeddings)

    write_jsonl(
        out / "passages.jsonl",
        ({"_id": i, "body": t} for i, t in zip(pass_ids, pass_texts)),
    )
    write_jsonl(
        out / "queries.jsonl",
        ({"_id": i, "text": t} for i, t in zip(query_ids, query_texts)),
    )
    (out / "pass_ids.txt").write_text("\n".join(pass_ids) + "\n", encoding="utf-8")
    (out / "query_ids.txt").write_text("\n".join(query_ids) + "\n", encoding="utf-8")
    meta = {
        "source": "MS MARCO Passage Ranking",
        "collection": str(Path(args.collection).resolve()),
        "queries": str(Path(args.queries).resolve()),
        "model": args.model,
        "dimensions": dimensions,
        "passages": len(pass_ids),
        "queries": len(query_ids),
        "normalized": True,
        "metric": "cosine",
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    print(f"DONE {out} {json.dumps(meta)}", flush=True)


if __name__ == "__main__":
    main()
