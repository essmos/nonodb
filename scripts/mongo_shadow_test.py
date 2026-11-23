#!/usr/bin/env python3
"""Smoke-test the opt-in MongoDB wire shadow mode.

The script starts a temporary NonoDB daemon, points its MongoDB wire listener at
a real MongoDB, performs CRUD through PyMongo, and checks that writes reached
both databases and that the shadow JSONL report was written.

    MONGODB_SHADOW_URL=mongodb://127.0.0.1:27017/?directConnection=true \
    NONODB_DAEMON=target/debug/nonodb-daemon python3 scripts/mongo_shadow_test.py
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]


def wait_for_port(port: int, process: subprocess.Popen[bytes], log_path: Path) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(
                f"daemon exited early: {log_path.read_text(errors='replace')[-2000:]}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise AssertionError("NonoDB MongoDB listener did not start")


def main() -> int:
    mongodb_url = os.environ.get("MONGODB_SHADOW_URL") or os.environ.get(
        "MONGODB_MIGRATE_URL"
    ) or os.environ.get("MONGODB_CONFORMANCE_URL")
    if not mongodb_url:
        print("MONGODB_SHADOW_URL is required", file=sys.stderr)
        return 2
    daemon = Path(os.environ.get("NONODB_DAEMON", "target/debug/nonodb-daemon"))
    if not daemon.is_absolute():
        daemon = ROOT / daemon
    if not daemon.exists():
        print(f"daemon binary not found: {daemon}", file=sys.stderr)
        return 2

    suffix = uuid.uuid4().hex[:10]
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    database = f"shadow_{suffix}"
    temp = Path(tempfile.mkdtemp(prefix="nonodb-shadow-"))
    log_path = temp / "shadow.jsonl"
    daemon_log = temp / "daemon.log"
    env = {
        **os.environ,
        "NONODB_DATA": str(temp / "data"),
        "NONODB_SOCK": str(temp / "nonodb.sock"),
        "NONODB_MONGO_ADDR": f"127.0.0.1:{port}",
        "NONODB_MONGO_SHADOW_URL": mongodb_url,
        "NONODB_MONGO_SHADOW_LOG": str(log_path),
    }
    process = subprocess.Popen(
        [str(daemon)],
        env=env,
        stdout=daemon_log.open("wb"),
        stderr=subprocess.STDOUT,
    )
    mongo = MongoClient(mongodb_url, serverSelectionTimeoutMS=5_000)
    nonodb = MongoClient(
        f"mongodb://127.0.0.1:{port}/?directConnection=true",
        serverSelectionTimeoutMS=5_000,
    )
    try:
        wait_for_port(port, process, daemon_log)
        source = mongo[database]
        target = nonodb[database]
        source.drop_collection("items")
        target.drop_collection("items")

        target.items.insert_many([{"_id": "a", "n": 1}, {"_id": "b", "n": 2}])
        target.items.update_one({"_id": "a"}, {"$set": {"n": 10}})
        target.items.delete_one({"_id": "b"})
        assert [(doc["_id"], doc["n"]) for doc in target.items.find({})] == [("a", 10)]

        assert [(doc["_id"], doc["n"]) for doc in source.items.find({})] == [("a", 10)]

        # Force a real difference: MongoDB already has this _id, NonoDB does not.
        source.items.insert_one({"_id": "conflict", "n": 1})
        target.items.insert_one({"_id": "conflict", "n": 1})
        source.items.insert_one({"_id": "only-mongodb", "n": 3})

        entries = [
            json.loads(line)
            for line in log_path.read_text().splitlines()
            if line.strip()
        ]
        commands = {entry["command"] for entry in entries}
        assert {"insert", "update", "delete"} <= commands, commands
        assert all("diff" in entry or "skipped" in entry or "error" in entry for entry in entries)
        assert any(entry.get("diff") for entry in entries), entries

        # Reads stay on NonoDB even though MongoDB has an extra document.
        assert target.items.count_documents({}) == 2
        assert source.items.count_documents({}) == 3
    finally:
        nonodb.close()
        try:
            mongo.drop_database(database)
        finally:
            mongo.close()
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    print("mongo shadow mode test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
