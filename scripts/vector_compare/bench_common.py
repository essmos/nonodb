"""Shared loading, recall, and latency helpers for the vector comparison."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterable

import numpy as np


def load_data(data: str | Path) -> tuple[np.ndarray, np.ndarray, list[str]]:
    data = Path(data)
    queries = np.load(data / "query_embeddings.npy")
    truth = np.load(data / "ground_truth.npy")
    query_ids = (data / "query_ids.txt").read_text(encoding="utf-8").splitlines()
    return queries, truth, query_ids


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * (len(ordered) - 1)))]


def recall_at_k(returned: Iterable, expected: Iterable) -> float:
    expected = list(expected)
    if not expected:
        return 0.0
    return len(set(returned) & set(expected)) / len(expected)


def benchmark(
    name: str,
    search,
    queries: np.ndarray,
    truth: np.ndarray,
    warmup: int,
    rounds: int,
    output: Path,
    config: dict,
) -> dict:
    """Run ``search(vector, k) -> ids`` and persist latency plus recall@10."""
    limit = int(truth.shape[1])
    for index in range(min(warmup, len(queries))):
        search(queries[index].tolist(), limit)

    latencies: list[float] = []
    recalls: list[float] = []
    total_started = time.perf_counter()
    for index in range(min(rounds, len(queries))):
        started = time.perf_counter()
        returned = search(queries[index].tolist(), limit)
        latencies.append((time.perf_counter() - started) * 1000.0)
        recalls.append(recall_at_k(returned[:limit], truth[index][:limit]))
    elapsed = time.perf_counter() - total_started

    result = {
        "engine": name,
        "queries": len(latencies),
        "limit": limit,
        "p50_ms": round(percentile(latencies, 0.50), 3),
        "p95_ms": round(percentile(latencies, 0.95), 3),
        "p99_ms": round(percentile(latencies, 0.99), 3),
        "mean_ms": round(sum(latencies) / len(latencies), 3),
        "max_ms": round(max(latencies), 3),
        "qps": round(len(latencies) / max(elapsed, 1e-9), 2),
        "recall_at_10": round(sum(recalls) / len(recalls), 4),
        "config": config,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)
    return result
