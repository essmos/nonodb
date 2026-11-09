"""Profil CPU daemona: ile rdzeni realnie używa zapis i odczyt.

    NONODB_SOCK=/tmp/nonodb.sock NONODB_PID=1234 \
        python3 scripts/cpu_bench.py --workers 12 --writes 24000 --reads 60000
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from bench import read_worker, write_worker  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
CLK = os.sysconf("SC_CLK_TCK")


def total_ticks(pid: int) -> int:
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().split()
    return int(parts[13]) + int(parts[14])


def thread_ticks(pid: int) -> dict[str, int]:
    out: dict[str, int] = {}
    try:
        tids = os.listdir(f"/proc/{pid}/task")
    except FileNotFoundError:
        return out
    for tid in tids:
        try:
            with open(f"/proc/{pid}/task/{tid}/stat") as f:
                line = f.read()
        except OSError:
            continue
        name = line[line.find("(") + 1 : line.rfind(")")]
        parts = line[line.rfind(")") + 2 :].split()
        out[name] = out.get(name, 0) + int(parts[11]) + int(parts[12])
    return out


class Sampler(threading.Thread):
    def __init__(self, pid: int):
        super().__init__(daemon=True)
        self.pid = pid
        self.stop = threading.Event()
        self.samples: list[int] = []

    def run(self) -> None:
        while not self.stop.is_set():
            self.samples.append(total_ticks(self.pid))
            time.sleep(0.05)

    def finish(self, t0: int, wall: float) -> float:
        self.stop.set()
        self.join()
        delta = total_ticks(self.pid) - t0
        return delta / (CLK * wall)


def run_phase(label: str, pid: int, fn) -> None:
    t0 = total_ticks(pid)
    threads0 = thread_ticks(pid)
    s = Sampler(pid)
    s.start()
    start = time.perf_counter()
    result = fn()
    wall = time.perf_counter() - start
    cores = s.finish(t0, wall)
    threads1 = thread_ticks(pid)
    deltas = {
        k: (threads1.get(k, 0) - threads0.get(k, 0)) / (CLK * wall)
        for k in set(threads0) | set(threads1)
    }
    top = sorted(deltas.items(), key=lambda kv: kv[1], reverse=True)[:5]
    print(
        f"{label:<28} {wall:6.2f}s  daemon={cores:5.2f} rdzeni  "
        f"({', '.join(f'{k}={v:.2f}' for k, v in top if v > 0.01)})"
    )
    return result, wall


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pid", type=int, default=int(os.environ.get("NONODB_PID", "0")))
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--writes", type=int, default=24000)
    ap.add_argument("--reads", type=int, default=60000)
    args = ap.parse_args()
    if not args.pid:
        print("podaj --pid lub NONODB_PID")
        return 1

    per_w = args.writes // args.workers
    per_r = args.reads // args.workers

    from nonodb import NonoDB

    db = NonoDB(SOCK, timeout=60)
    db.collection("cpu_bench").delete({}, multi=True)
    db.close()

    def writes():
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            fs = [ex.submit(write_worker, w, per_w) for w in range(args.workers)]
            return [f.result() for f in fs]

    (results, wall_w) = run_phase("ZAPIS (12 procesów)", args.pid, writes)
    total_w = sum(len(r[1]) for r in results)
    ids = [i for r in results for i in r[0]]
    print(f"  -> {total_w / wall_w:,.0f} ops/s")

    (_, wall_r) = run_phase(
        "ODCZYT _id (12 procesów)",
        args.pid,
        lambda: _reads(ids, per_r, args.workers),
    )
    print(f"  -> {args.reads / wall_r:,.0f} ops/s")

    # Skalowanie odczytu: 4 vs 12 procesów.
    for w in (4, 12):
        (_, wall) = run_phase(
            f"ODCZYT _id ({w} procesów)",
            args.pid,
            lambda w=w: _reads(ids, args.reads // w, w),
        )
        print(f"  -> {args.reads / wall:,.0f} ops/s")
    return 0


def _reads(ids, reps, workers):
    with ProcessPoolExecutor(max_workers=workers) as ex:
        fs = [ex.submit(read_worker, ids, reps) for _ in range(workers)]
        return [f.result() for f in fs]


if __name__ == "__main__":
    raise SystemExit(main())
