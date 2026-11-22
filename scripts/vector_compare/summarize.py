"""Render vector comparison result JSON files as a Markdown table."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", help="directory with *.json result files")
    parser.add_argument("--sort", default="p50_ms")
    args = parser.parse_args()

    rows = []
    for path in sorted(Path(args.results).glob("*.json")):
        row = json.loads(path.read_text(encoding="utf-8"))
        if "engine" in row and "p50_ms" in row:
            rows.append(row)
    rows.sort(key=lambda row: row.get(args.sort, 0))

    print("| Engine | Queries | p50 ms | p95 ms | p99 ms | QPS | Recall@10 |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for row in rows:
        print(
            f"| {row['engine']} | {row['queries']} | {row['p50_ms']:.2f} | "
            f"{row['p95_ms']:.2f} | {row['p99_ms']:.2f} | {row['qps']:.1f} | "
            f"{row['recall_at_10']:.4f} |"
        )
    print()
    for row in rows:
        config = row.get("config", {})
        print(f"- **{row['engine']}**: `{json.dumps(config, sort_keys=True)}`")


if __name__ == "__main__":
    main()
