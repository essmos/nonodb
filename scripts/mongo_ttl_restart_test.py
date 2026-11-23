#!/usr/bin/env python3
"""Verify MongoDB TTL-index translation survives a daemon restart.

Starts a temporary NonoDB daemon, creates a TTL index through PyMongo, restarts
the daemon on the same data directory, and checks that the date-field mode and
per-document expiry are still enforced. No external MongoDB server is needed.

    cargo build --locked --bins
    python3 scripts/mongo_ttl_restart_test.py
"""

from __future__ import annotations

import datetime as dt
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
sys.path.insert(0, str(ROOT / "client"))
from nonodb import NonoDB  # noqa: E402


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_tcp(port: int, proc: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited with {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("daemon did not open the MongoDB listener")


def main() -> int:
    if not DAEMON.is_file():
        raise SystemExit("missing nonodb-daemon; run cargo build --locked --bins")
    with tempfile.TemporaryDirectory(prefix="nonodb-mongo-ttl-restart-") as raw:
        tmp = Path(raw)
        data = tmp / "data"
        data.mkdir()
        sock = tmp / "nonodb.sock"
        port = free_port()
        url = f"mongodb://127.0.0.1:{port}/?directConnection=true"
        logs = (tmp / "daemon.log").open("wb")
        procs: list[subprocess.Popen[bytes]] = []

        def start() -> subprocess.Popen[bytes]:
            env = os.environ.copy()
            env.update(
                NONODB_DATA=str(data),
                NONODB_SOCK=str(sock),
                NONODB_MONGO_ADDR=f"127.0.0.1:{port}",
                NONODB_TTL_SWEEP_INTERVAL="3600",
                NONODB_LOG="warn",
            )
            proc = subprocess.Popen([str(DAEMON)], env=env, stdout=logs, stderr=subprocess.STDOUT)
            procs.append(proc)
            wait_tcp(port, proc)
            return proc

        def stop(proc: subprocess.Popen[bytes]) -> None:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

        try:
            proc = start()
            client = MongoClient(url, serverSelectionTimeoutMS=5_000)
            db = client["mongo_ttl_restart"]
            collection = db["sessions"]
            now = dt.datetime.now(dt.timezone.utc)
            collection.insert_many(
                [
                    {"_id": "expired", "createdAt": now - dt.timedelta(hours=1)},
                    {"_id": "live", "createdAt": now + dt.timedelta(hours=1)},
                    {"_id": "missing"},
                ]
            )
            assert (
                collection.create_index(
                    [("createdAt", 1)], expireAfterSeconds=60, name="created_ttl"
                )
                == "created_ttl"
            )
            nd = NonoDB(socket_path=str(sock), database="mongo_ttl_restart")
            assert nd.get_collection_ttl("sessions") == 60
            assert nd.get_collection_ttl_field("sessions") == "createdAt"
            assert collection.find_one({"_id": "expired"}) is None
            assert collection.find_one({"_id": "live"}) is not None
            client.close()
            stop(proc)

            proc = start()
            client = MongoClient(url, serverSelectionTimeoutMS=5_000)
            db = client["mongo_ttl_restart"]
            collection = db["sessions"]
            nd = NonoDB(socket_path=str(sock), database="mongo_ttl_restart")
            assert nd.get_collection_ttl("sessions") == 60
            assert nd.get_collection_ttl_field("sessions") == "createdAt"
            assert collection.find_one({"_id": "expired"}) is None
            assert collection.find_one({"_id": "live"}) is not None
            assert collection.find_one({"_id": "missing"}) is not None
            indexes = collection.index_information()
            assert "created_ttl" in indexes, indexes
            client.close()
            stop(proc)
            print("mongo TTL restart test: ok")
            return 0
        finally:
            for proc in procs:
                if proc.poll() is None:
                    stop(proc)
            logs.close()


if __name__ == "__main__":
    raise SystemExit(main())
