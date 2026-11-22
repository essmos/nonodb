"""Large-scale BM25, fuzzy, and exact-vector benchmark.

The default data set is 10M short documents with a BM25 index on ``body``, a
fuzzy index on ``title``, a 16-dimensional vector index on ``vec``, and an
indexed ``cat`` field.

    NONODB_SOCK=/tmp/nonodb.sock \
      python3 scripts/search_scale_bench.py --docs 10000000

Start the daemon with ``NONODB_CACHE_TTL=0``. This script repeats the same
query, so the default read cache would otherwise report cache-hit latency.

Use ``--queries-only`` to reuse an existing collection after the indexes have
already been built.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLIENT = os.environ.get("NONODB_CLIENT") or os.path.join(ROOT, "client")
sys.path.insert(0, CLIENT)

from nonodb import NonoDB  # noqa: E402

WORDS = [
    "database",
    "storage",
    "engine",
    "rust",
    "async",
    "search",
    "index",
    "query",
    "memory",
    "latency",
    "postgresql",
    "python",
]
BODIES = [
    " ".join(WORDS[(i + j * 5) % len(WORDS)] for j in range(8)) for i in range(1024)
]
TITLES = [f"w{i:05d}" for i in range(10_000)]
VECS = [
    [((i * 31 + j * 17) % 1000) / 1000.0 for j in range(16)] for i in range(1024)
]


def make_doc(i: int) -> dict:
    return {
        "_id": f"s{i:09d}",
        "body": BODIES[i % len(BODIES)],
        "title": TITLES[i % len(TITLES)],
        "vec": VECS[i % len(VECS)],
        "cat": i % 10,
    }


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * (len(ordered) - 1)))] * 1000.0


def data_size(data_dir: str) -> str:
    path = os.path.join(data_dir, "default", "data.mdb")
    try:
        return f"{os.path.getsize(path) / 1e9:.2f} GB"
    except OSError:
        return "unknown"


def clear_collection(c) -> None:
    for field in c.list_text_indexes():
        c.drop_text_index(field)
    for field in c.list_fuzzy_indexes():
        c.drop_fuzzy_index(field)
    for index in c.list_vector_indexes():
        c.drop_vector_index(index["field"])
    for index in c.list_indexes():
        c.drop_index("+".join(index["fields"]))
    try:
        c.delete({}, multi=True)
    except Exception:  # noqa: BLE001
        pass


def wait_build(handle, label: str, timeout: float) -> tuple[bool, float]:
    started = time.perf_counter()
    last_report = 0.0
    while True:
        elapsed = time.perf_counter() - started
        active = [build for build in handle.status() if not build.get("finished")]
        if not active:
            if label == "text":
                exists = handle.target in handle._db.collection(handle.collection).list_text_indexes()
            elif label == "fuzzy":
                exists = handle.target in handle._db.collection(handle.collection).list_fuzzy_indexes()
            else:
                exists = True
            if exists:
                return True, elapsed
        if elapsed >= timeout:
            print(f"{label} index TIMEOUT after {elapsed:.0f}s", flush=True)
            return False, elapsed
        if elapsed - last_report >= 30:
            if active:
                build = active[0]
                done = build.get("done", build.get("processed", "?"))
                total = build.get("total", "?")
                print(
                    f"{label} index progress {done}/{total} at {elapsed:.0f}s",
                    flush=True,
                )
            last_report = elapsed
        time.sleep(5)


def measure(label: str, fn, *, iters: int, warmup: int = 1) -> None:
    try:
        for _ in range(warmup):
            fn()
        latencies = []
        rows = 0
        for _ in range(iters):
            started = time.perf_counter()
            result = fn()
            latencies.append(time.perf_counter() - started)
            rows = len(result)
        print(
            f"{label:<44} rows={rows:>4} p50={pct(latencies, 0.5):9.2f}ms "
            f"p95={pct(latencies, 0.95):9.2f}ms",
            flush=True,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"{label:<44} ERROR {type(exc).__name__}: {exc}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", type=int, default=10_000_000)
    parser.add_argument("--batch", type=int, default=20_000)
    parser.add_argument("--collection", default="search10m")
    parser.add_argument("--data-dir", default=os.environ.get("NONODB_DATA", ""))
    parser.add_argument("--index-timeout", type=float, default=14_400)
    parser.add_argument("--query-timeout", type=float, default=600)
    parser.add_argument("--queries-only", action="store_true")
    parser.add_argument("--skip-load", action="store_true")
    parser.add_argument("--skip-fuzzy", action="store_true")
    parser.add_argument("--skip-vector", action="store_true")
    parser.add_argument("--text-iters", type=int, default=3)
    parser.add_argument("--vector-iters", type=int, default=3)
    parser.add_argument("--fuzzy-iters", type=int, default=1)
    args = parser.parse_args()

    sock = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
    db = NonoDB(sock, timeout=args.query_timeout)
    c = db.collection(args.collection)

    if not args.queries_only:
        if not args.skip_load:
            print(f"clearing {args.collection}", flush=True)
            clear_collection(c)

            started = time.perf_counter()
            last = started
            for offset in range(0, args.docs, args.batch):
                end = min(offset + args.batch, args.docs)
                c.insert_many(make_doc(i) for i in range(offset, end))
                if end % 5_000_000 == 0:
                    now = time.perf_counter()
                    print(
                        f"LOAD {end:,}/{args.docs:,} {end / (now - started):,.0f} docs/s "
                        f"chunk={now - last:.1f}s data={data_size(args.data_dir)}",
                        flush=True,
                    )
                    last = now
            load = time.perf_counter() - started
            print(
                f"LOAD_DONE {args.docs:,} in {load:.1f}s -> {args.docs / load:,.0f} docs/s "
                f"data={data_size(args.data_dir)}",
                flush=True,
            )
            print(f"COUNT {c.count({}):,}", flush=True)

            started = time.perf_counter()
            c.create_index("cat")
            print(
                f"INDEX cat in {time.perf_counter() - started:.1f}s "
                f"data={data_size(args.data_dir)}",
                flush=True,
            )
        else:
            print(f"SKIP load; count={c.count({}):,}", flush=True)

        started = time.perf_counter()
        text = c.create_text_index("body", wait=False)
        text_ok, _ = wait_build(text, "text", args.index_timeout)
        print(
            f"INDEX text in {time.perf_counter() - started:.1f}s "
            f"data={data_size(args.data_dir)}",
            flush=True,
        )

        fuzzy_ok = False
        if not args.skip_fuzzy:
            started = time.perf_counter()
            fuzzy = c.create_fuzzy_index("title", wait=False)
            fuzzy_ok, _ = wait_build(fuzzy, "fuzzy", args.index_timeout)
            print(
                f"INDEX fuzzy in {time.perf_counter() - started:.1f}s "
                f"data={data_size(args.data_dir)}",
                flush=True,
            )

        vector_ok = False
        if not args.skip_vector:
            started = time.perf_counter()
            try:
                db.exec(
                    {
                        "op": "create_vector_index",
                        "coll": args.collection,
                        "field": "vec",
                        "dims": 16,
                        "metric": "cosine",
                    },
                    timeout=args.index_timeout,
                )
                vector_ok = True
            except Exception as exc:  # noqa: BLE001
                print(f"INDEX vector ERROR {type(exc).__name__}: {exc}", flush=True)
            print(
                f"INDEX vector in {time.perf_counter() - started:.1f}s "
                f"data={data_size(args.data_dir)}",
                flush=True,
            )
    else:
        text_ok = bool(c.list_text_indexes())
        fuzzy_ok = bool(c.list_fuzzy_indexes()) and not args.skip_fuzzy
        vector_ok = bool(c.list_vector_indexes()) and not args.skip_vector

    print("QUERIES", flush=True)
    if text_ok:
        measure(
            "BM25 single term",
            lambda: c.search("body", "database", limit=10),
            iters=args.text_iters,
        )
        measure(
            "BM25 two terms",
            lambda: c.search("body", "database storage", limit=10),
            iters=args.text_iters,
        )
        measure(
            "BM25 three terms",
            lambda: c.search("body", "database storage engine", limit=10),
            iters=args.text_iters,
        )
        measure(
            "BM25 + indexed cat filter",
            lambda: c.search("body", "database", filter={"cat": 1}, limit=10),
            iters=args.text_iters,
        )
    if fuzzy_ok:
        measure(
            "fuzzy title distance 1",
            lambda: c.fuzzy("title", "w0123", max_distance=1, limit=10),
            iters=args.fuzzy_iters,
        )
        measure(
            "fuzzy title distance 2",
            lambda: c.fuzzy("title", "w0123", max_distance=2, limit=10),
            iters=args.fuzzy_iters,
        )
    if vector_ok:
        measure(
            "vector 16D no filter",
            lambda: c.vector_search("vec", VECS[0], limit=10),
            iters=args.vector_iters,
        )
        measure(
            "vector 16D indexed cat filter",
            lambda: c.vector_search("vec", VECS[0], filter={"cat": 1}, limit=10),
            iters=args.vector_iters,
        )

    print(f"FINAL data={data_size(args.data_dir)}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
