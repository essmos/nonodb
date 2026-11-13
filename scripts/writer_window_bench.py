#!/usr/bin/env python3
"""Compare adaptive/fixed group-commit windows for single-client writes."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "client"))
from nonodb import NonoDB  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/release/nonodb-daemon"))

CONFIGS = {
    "old_fixed_1ms": {"NONODB_WRITER_WINDOW_IDLE_US": "1000"},
    "adaptive_100us": {"NONODB_WRITER_WINDOW_IDLE_US": "100"},
    "low_latency_10us": {
        "NONODB_WRITER_WINDOW_IDLE_US": "10",
        "NONODB_WRITER_WINDOW_US": "200",
    },
    "throughput_2ms": {
        "NONODB_WRITER_WINDOW_IDLE_US": "100",
        "NONODB_WRITER_WINDOW_US": "2000",
        "NONODB_WRITER_WINDOW_MAX_US": "5000",
    },
}


def stats(samples: list[float]) -> dict:
    samples.sort()
    return {
        "p50_ms": round(samples[len(samples) // 2], 3),
        "p95_ms": round(samples[int(len(samples) * 0.95)], 3),
    }


def run(name: str, env_extra: dict[str, str], writes: int) -> dict:
    temp = Path(tempfile.mkdtemp(prefix=f"nonodb-window-{name}-"))
    data = temp / "data"
    data.mkdir()
    sock = temp / "nonodb.sock"
    env = os.environ.copy()
    env.update(
        NONODB_DATA=str(data),
        NONODB_SOCK=str(sock),
        NONODB_CACHE_TTL="0",
        NONODB_LOG_FORMAT="json",
        RUST_LOG="warn",
        **env_extra,
    )
    log = open(temp / "daemon.log", "w", encoding="utf-8")
    proc = subprocess.Popen(
        [str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT
    )
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not sock.exists():
            if proc.poll() is not None:
                raise RuntimeError(f"daemon exited with {proc.returncode}")
            time.sleep(0.05)
        db = NonoDB(str(sock), cache_ttl=0)
        collection = db.collection("w")
        samples = []
        started = time.perf_counter()
        for i in range(writes):
            t0 = time.perf_counter()
            collection.insert({"_id": f"{name}-{i}", "v": i}, durability="full")
            samples.append((time.perf_counter() - t0) * 1000.0)
        elapsed = time.perf_counter() - started
        db.close()
        out = stats(samples)
        out["ops_per_s"] = round(writes / elapsed, 1)
        return out
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        log.close()
        shutil.rmtree(temp, ignore_errors=True)


def main() -> int:
    if not DAEMON.is_file():
        raise SystemExit(f"missing daemon binary: {DAEMON}")
    writes = int(os.environ.get("WINDOW_BENCH_WRITES", "300"))
    results = {
        name: run(name, env, writes)
        for name, env in CONFIGS.items()
    }
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
