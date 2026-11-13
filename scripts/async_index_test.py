#!/usr/bin/env python3
"""Deferred non-unique index worker test.

Starts with the worker disabled to verify that queries fall back to scans while
changes are pending, then restarts with the worker enabled and checks that the
durable queue drains and the index becomes usable.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))
from nonodb import NonoDB, NonoDBError  # noqa: E402

DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))


def start(data: Path, sock: Path, log: Path, *, worker: bool):
    env = os.environ.copy()
    env.update(
        NONODB_DATA=str(data),
        NONODB_SOCK=str(sock),
        NONODB_ASYNC_INDEXES="1",
        NONODB_INDEX_WORKER="1" if worker else "0",
        NONODB_CACHE_TTL="0",
        NONODB_LOG_FORMAT="json",
        RUST_LOG="warn",
    )
    handle = open(log, "a", encoding="utf-8")
    proc = subprocess.Popen(
        [str(DAEMON)],
        env=env,
        stdout=handle,
        stderr=subprocess.STDOUT,
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if sock.exists():
            return proc, handle
        if proc.poll() is not None:
            handle.close()
            raise RuntimeError(f"daemon exited with code {proc.returncode}")
        time.sleep(0.05)
    raise RuntimeError("daemon socket did not appear")


def metrics(sock: Path) -> dict:
    with httpx.Client(
        transport=httpx.HTTPTransport(uds=str(sock)),
        base_url="http://localhost",
        timeout=5,
    ) as client:
        return client.get("/metrics").json()


def wait_pending(sock: Path, predicate, timeout: float = 15.0) -> dict:
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        last = metrics(sock)
        if predicate(last):
            return last
        time.sleep(0.05)
    raise AssertionError(f"pending condition not met: {json.dumps(last, sort_keys=True)}")


def stop(proc: subprocess.Popen, handle) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
    handle.close()


def main() -> int:
    if not DAEMON.is_file():
        raise SystemExit(f"missing daemon binary: {DAEMON}; run cargo build")
    temp = Path(tempfile.mkdtemp(prefix="nonodb-async-index-"))
    data = temp / "data"
    data.mkdir()
    sock = temp / "nonodb.sock"
    log = temp / "daemon.log"

    proc = handle = None
    try:
        proc, handle = start(data, sock, log, worker=False)
        db = NonoDB(str(sock), cache_ttl=0)
        users = db.collection("users")
        users.create_index("age")
        users.create_index("email", unique=True)
        users.insert_many(
            [
                {"_id": f"u{i}", "age": i % 10, "email": f"u{i}@example.test"}
                for i in range(100)
            ]
        )

        pending = wait_pending(sock, lambda m: m.get("index_queue_pending", 0) > 0)
        assert pending["index_queue_pending"] == 100, pending["index_queue_pending"]
        assert len(users.find({"age": 5})) == 10
        assert users.explain({"age": 5})["access_path"] == "collection_scan"

        users.update({"age": 5}, {"$set": {"age": 6}}, multi=True)
        users.delete({"_id": "u6"})
        assert len(users.find({"age": 5})) == 0
        assert len(users.find({"age": 6})) == 19

        try:
            users.insert({"_id": "dupe", "age": 1, "email": "u1@example.test"})
        except NonoDBError as exc:
            assert exc.code == "duplicate_key", exc
        else:
            raise AssertionError("unique index was not enforced synchronously")
        db.close()
        stop(proc, handle)
        proc = handle = None

        proc, handle = start(data, sock, log, worker=True)
        drained = wait_pending(sock, lambda m: m.get("index_queue_pending", 0) == 0)
        assert drained["index_queue_processed_total"] >= 111, drained
        db = NonoDB(str(sock), cache_ttl=0)
        users = db.collection("users")
        assert len(users.find({"age": 5})) == 0
        assert len(users.find({"age": 6})) == 19
        assert users.explain({"age": 6})["access_path"] == "index_scan"
        db.close()
        print("async index worker: ok")
        return 0
    finally:
        if proc is not None:
            stop(proc, handle)
        shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
