#!/usr/bin/env python3
"""Small latency smoke test for unified search.

This is not a competitive benchmark. It guards the read path against accidental
full scans/regressions in lexical, vector, and hybrid search.
"""

from __future__ import annotations

import os
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402

DOCS = 1000
QUERIES = 25


def daemon_binary() -> Path:
    configured = os.environ.get("NONODB_DAEMON")
    path = Path(configured) if configured else ROOT / "target" / "debug" / "nonodb-daemon"
    if not path.is_file():
        path = ROOT / "target" / "release" / "nonodb-daemon"
    if not path.is_file():
        raise SystemExit(f"missing daemon binary: {path}")
    return path


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * fraction))
    return ordered[index]


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="nonodb-search-bench-") as tmp_name:
        tmp = Path(tmp_name)
        sock = tmp / "nonodb.sock"
        env = os.environ.copy()
        env.update(
            {
                "NONODB_DATA": str(tmp / "data"),
                "NONODB_SOCK": str(sock),
                "NONODB_MAP_SIZE": "128MiB",
                "RUST_LOG": "warn",
            }
        )
        log = (tmp / "daemon.log").open("wb")
        proc = subprocess.Popen(
            [str(daemon_binary())], env=env, stdout=log, stderr=subprocess.STDOUT
        )
        try:
            deadline = time.time() + 20
            while time.time() < deadline and not sock.exists():
                if proc.poll() is not None:
                    raise RuntimeError("daemon exited during startup")
                time.sleep(0.05)
            if not sock.exists():
                raise RuntimeError("daemon socket did not appear")
            db = NonoDB(str(sock), timeout=30)
            products = db.collection("products")
            docs = []
            for index in range(DOCS):
                docs.append(
                    {
                        "_id": f"p{index}",
                        "title": "coffee grinder" if index % 3 == 0 else "tea kettle",
                        "body": "fresh roasted coffee beans and equipment",
                        "status": "active",
                        "embedding": [
                            float(index % 17),
                            float((index * 3) % 19),
                            1.0,
                            0.5,
                        ],
                    }
                )
                if len(docs) >= 200:
                    products.insert_many(docs)
                    docs = []
            if docs:
                products.insert_many(docs)

            started = time.perf_counter()
            products.create_text_index("title")
            products.create_text_index("body")
            products.create_vector_index("embedding", 4, "cosine")
            index_ms = (time.perf_counter() - started) * 1000

            cases = {
                "lexical": lambda: products.search_all(
                    mode="lexical",
                    query="coffee beans",
                    fields=["title", "body"],
                    limit=10,
                ),
                "vector": lambda: products.search_all(
                    mode="vector",
                    vector=[3.0, 2.0, 1.0, 0.5],
                    vector_field="embedding",
                    limit=10,
                ),
                "hybrid": lambda: products.search_all(
                    mode="hybrid",
                    query="coffee beans",
                    fields=["title", "body"],
                    vector=[3.0, 2.0, 1.0, 0.5],
                    vector_field="embedding",
                    limit=10,
                ),
            }
            print(f"indexed {DOCS} docs in {index_ms:.1f} ms")
            failed = False
            for name, query in cases.items():
                timings = []
                for _ in range(QUERIES):
                    started = time.perf_counter()
                    results = query()
                    elapsed = (time.perf_counter() - started) * 1000
                    timings.append(elapsed)
                    assert len(results) == 10, (name, len(results))
                p50 = statistics.median(timings)
                p95 = percentile(timings, 0.95)
                print(f"{name:7s} p50={p50:.2f} ms p95={p95:.2f} ms")
                if p95 > 2000:
                    failed = True
                    print(f"{name}: p95 above 2000 ms", file=sys.stderr)
            db.close()
            if failed:
                return 1
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            log.close()
    print("search bench: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
