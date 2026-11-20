"""Load the MS MARCO passage collection into NonoDB.

The official collection is a TSV file with ``passage_id \t passage``. This
script streams it in bounded batches, reports progress, and optionally builds a
BM25 index on the ``body`` field.

    NONODB_SOCK=/tmp/nonodb.sock \
      python3 scripts/load_msmarco.py /path/to/collection.tsv --collection msmarco

The script does not download the dataset. Get it from the MS MARCO passage
ranking archive and verify its checksum before loading.
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


def data_size(data_dir: str) -> str:
    path = os.path.join(data_dir, "default", "data.mdb")
    try:
        return f"{os.path.getsize(path) / 1e9:.2f} GB"
    except OSError:
        return "unknown"


def wait_build(handle, label: str, timeout: float) -> bool:
    started = time.perf_counter()
    last_report = 0.0
    while True:
        elapsed = time.perf_counter() - started
        active = [build for build in handle.status() if not build.get("finished")]
        if not active:
            return True
        if elapsed >= timeout:
            print(f"{label} index TIMEOUT after {elapsed:.0f}s", flush=True)
            return False
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("tsv", help="path to MS MARCO collection.tsv")
    parser.add_argument("--collection", default="msmarco")
    parser.add_argument("--batch", type=int, default=2000)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--data-dir", default=os.environ.get("NONODB_DATA", ""))
    parser.add_argument("--query-timeout", type=float, default=1800)
    parser.add_argument("--index-timeout", type=float, default=14_400)
    parser.add_argument("--skip-index", action="store_true")
    parser.add_argument("--recreate", action="store_true")
    args = parser.parse_args()

    sock = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
    db = NonoDB(sock, timeout=args.query_timeout)
    c = db.collection(args.collection)

    if args.recreate:
        for field in c.list_text_indexes():
            c.drop_text_index(field)
        c.delete({}, multi=True)
        print("recreated collection", flush=True)

    started = time.perf_counter()
    last = started
    loaded = 0
    batch: list[dict] = []

    def flush() -> None:
        nonlocal loaded, last, batch
        if not batch:
            return
        c.insert_many(batch)
        loaded += len(batch)
        batch = []
        if loaded % 1_000_000 < args.batch:
            now = time.perf_counter()
            print(
                f"LOAD {loaded:,} {loaded / (now - started):,.0f} docs/s "
                f"chunk={now - last:.1f}s data={data_size(args.data_dir)}",
                flush=True,
            )
            last = now

    with open(args.tsv, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if args.limit and loaded + len(batch) >= args.limit:
                break
            passage_id, sep, passage = line.rstrip("\n").partition("\t")
            if not sep:
                continue
            batch.append({"_id": passage_id, "body": passage})
            if len(batch) >= args.batch:
                flush()
    flush()

    elapsed = time.perf_counter() - started
    print(
        f"LOAD_DONE {loaded:,} in {elapsed:.1f}s -> {loaded / elapsed:,.0f} docs/s "
        f"data={data_size(args.data_dir)}",
        flush=True,
    )

    if not args.skip_index:
        started = time.perf_counter()
        build = c.create_text_index("body", wait=False)
        ok = wait_build(build, "text", args.index_timeout)
        print(
            f"INDEX text {'ok' if ok else 'failed'} in "
            f"{time.perf_counter() - started:.1f}s data={data_size(args.data_dir)}",
            flush=True,
        )
    db.close()


if __name__ == "__main__":
    main()
