#!/usr/bin/env python3
"""Durability modes, explicit sync, and crash/restart checks.

    cargo build -p nonodb-daemon
    python3 scripts/durability_test.py
"""

from __future__ import annotations

import os
import signal
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))


def wait_socket(path: Path, proc) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited with {proc.returncode}")
        time.sleep(0.05)
    raise RuntimeError("daemon socket did not appear")


def start(data: Path, sock: Path, *, unsafe: bool, log: Path):
    env = os.environ.copy()
    env.update(
        NONODB_DATA=str(data),
        NONODB_SOCK=str(sock),
        NONODB_LOG="warn",
        NONODB_CACHE_TTL="0",
    )
    if unsafe:
        env["NONODB_ALLOW_UNSAFE_WRITES"] = "1"
    else:
        env.pop("NONODB_ALLOW_UNSAFE_WRITES", None)
    fh = log.open("ab")
    proc = __import__("subprocess").Popen(
        [str(DAEMON)], env=env, stdout=fh, stderr=__import__("subprocess").STDOUT
    )
    fh.close()
    wait_socket(sock, proc)
    return proc


def crash(proc) -> None:
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=10)


def stop(proc) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except Exception:
        proc.kill()


def main() -> int:
    if not DAEMON.is_file():
        raise SystemExit(f"missing daemon binary: {DAEMON}")
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-durability-", dir="/var/tmp"))
    data = tmp / "data"
    sock = tmp / "nonodb.sock"
    log = tmp / "daemon.log"
    proc = start(data, sock, unsafe=False, log=log)
    try:
        db = NonoDB(str(sock), cache_ttl=0.0)
        c = db.collection("durability")
        c.insert({"_id": "full", "mode": "full"}, durability="full")
        c.insert({"_id": "relaxed", "mode": "relaxed"}, durability="relaxed")
        try:
            c.insert({"_id": "unsafe-denied"}, durability="unsafe")
        except NonoDBError as exc:
            assert exc.code == "unsafe_durability_disabled", exc
        else:
            raise AssertionError("unsafe write was accepted without opt-in")
        assert db.sync()["synced"] is True
        assert c.count({}) == 2
        db.close()
    finally:
        stop(proc)

    # Full and explicitly synced relaxed writes must survive SIGKILL.
    proc = start(data, sock, unsafe=False, log=log)
    try:
        db = NonoDB(str(sock), cache_ttl=0.0)
        c = db.collection("durability")
        assert c.find_one({"_id": "full"}) is not None
        assert c.find_one({"_id": "relaxed"}) is not None
        # Full durability must survive SIGKILL immediately after the response,
        # without an explicit sync call.
        c.insert({"_id": "full-crash", "mode": "full"}, durability="full")
        db.close()
    finally:
        crash(proc)

    proc = start(data, sock, unsafe=False, log=log)
    try:
        db = NonoDB(str(sock), cache_ttl=0.0)
        c = db.collection("durability")
        assert c.find_one({"_id": "full-crash"}) is not None

        c.write_batch(
            [
                {"op": "insert", "doc": {"_id": "batch-relaxed"}},
                {"op": "update", "filter": {"_id": "full"}, "update": {"$set": {"n": 1}}},
            ],
            durability="relaxed",
        )
        db.exec({
            "op": "sql",
            "query": "INSERT INTO durability (_id, mode) VALUES ('sql-relaxed', 'relaxed')",
            "durability": "relaxed",
        })
        db.sync()
        db.close()
    finally:
        crash(proc)

    proc = start(data, sock, unsafe=False, log=log)
    try:
        db = NonoDB(str(sock), cache_ttl=0.0)
        c = db.collection("durability")
        assert c.find_one({"_id": "batch-relaxed"}) is not None
        assert c.find_one({"_id": "sql-relaxed"}) is not None
        db.close()
    finally:
        stop(proc)

    # Unsafe is opt-in and works when explicitly enabled.
    proc = start(data, sock, unsafe=True, log=log)
    try:
        db = NonoDB(str(sock), cache_ttl=0.0)
        c = db.collection("durability")
        c.insert({"_id": "unsafe", "mode": "unsafe"}, durability="unsafe")
        assert c.find_one({"_id": "unsafe"}) is not None
        db.sync()
        db.close()
    finally:
        crash(proc)

    proc = start(data, sock, unsafe=True, log=log)
    try:
        db = NonoDB(str(sock), cache_ttl=0.0)
        assert db.collection("durability").find_one({"_id": "unsafe"}) is not None

        # Informational throughput comparison on the same data directory.
        n = 1_000
        for mode in ("full", "relaxed", "unsafe"):
            coll = db.collection(f"single_{mode}")
            coll.delete({}, multi=True)
            started = time.perf_counter()
            for i in range(n):
                coll.insert({"i": i, "payload": "x" * 64}, durability=mode)
            elapsed = time.perf_counter() - started
            print(f"single {mode:<8} {n} docs in {elapsed:.3f}s = {n / elapsed:,.0f} docs/s")

        n = 10_000
        docs = [{"i": i, "payload": "x" * 64} for i in range(n)]
        for mode in ("full", "relaxed", "unsafe"):
            coll = db.collection(f"bench_{mode}")
            coll.delete({}, multi=True)
            started = time.perf_counter()
            for offset in range(0, n, 1000):
                coll.insert_many(docs[offset : offset + 1000], durability=mode)
            elapsed = time.perf_counter() - started
            print(f"{mode:<8} {n} docs in {elapsed:.3f}s = {n / elapsed:,.0f} docs/s")
        db.close()
    finally:
        stop(proc)

    print("durability test: ok")
    import shutil

    shutil.rmtree(tmp, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
