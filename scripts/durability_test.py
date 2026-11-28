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
    import socket

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited with {proc.returncode}")
        # A killed daemon leaves its socket file behind, so a plain `exists()`
        # check races the restart; only a successful connect means "listening".
        if path.exists():
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                    probe.settimeout(0.2)
                    probe.connect(str(path))
                return
            except OSError:
                pass
        time.sleep(0.05)
    raise RuntimeError("daemon socket did not appear")


def start(data: Path, sock: Path, *, unsafe: bool, log: Path, interval_ms: int | None = None):
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
    if interval_ms:
        env["NONODB_SYNC_INTERVAL_MS"] = str(interval_ms)
    else:
        env.pop("NONODB_SYNC_INTERVAL_MS", None)
    fh = log.open("ab")
    proc = __import__("subprocess").Popen(
        [str(DAEMON)], env=env, stdout=fh, stderr=__import__("subprocess").STDOUT
    )
    fh.close()
    wait_socket(sock, proc)
    return proc


def sync_runs(sock: Path) -> int | None:
    """Reads `sync_interval_runs_total` from the daemon metrics endpoint."""
    import httpx

    transport = httpx.HTTPTransport(uds=str(sock))
    with httpx.Client(transport=transport, base_url="http://localhost", timeout=10) as client:
        body = client.get("/metrics").json()
    return body.get("sync_interval_runs_total")


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

    # Interval syncing: with NONODB_SYNC_INTERVAL_MS the environment runs
    # MDB_NOSYNC and a background thread syncs on a timer, so a relaxed write
    # survives SIGKILL once the window has passed - and nothing more.
    interval_data = tmp / "interval"
    interval_sock = tmp / "interval.sock"
    proc = start(interval_data, interval_sock, unsafe=False, log=log, interval_ms=50)
    try:
        db = NonoDB(str(interval_sock), cache_ttl=0.0)
        c = db.collection("interval")
        for i in range(200):
            c.insert({"i": i, "payload": "x" * 64}, durability="relaxed")
        time.sleep(0.6)  # several windows: the syncer must have run by now
        runs = sync_runs(interval_sock)
        assert runs and runs > 0, f"interval syncer never ran: {runs}"
        db.close()
    finally:
        crash(proc)

    proc = start(interval_data, interval_sock, unsafe=False, log=log, interval_ms=50)
    try:
        db = NonoDB(str(interval_sock), cache_ttl=0.0)
        stored = db.collection("interval").count({})
        db.close()
    finally:
        stop(proc)
    assert stored == 200, f"interval syncing lost durable-by-contract writes: {stored}/200"
    print(f"interval sync: {runs} background syncs, 200/200 relaxed writes survived SIGKILL")

    print("durability test: ok")
    import shutil

    shutil.rmtree(tmp, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
