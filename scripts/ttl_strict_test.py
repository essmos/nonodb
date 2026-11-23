#!/usr/bin/env python3
"""E2E strict read-time TTL visibility before the background sweep runs.

The daemon is started with a very long sweep interval, so every assertion below
proves filtering at read time rather than deferred physical deletion.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402

COLL = "ttl_strict"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def daemon_binary() -> Path:
    configured = os.environ.get("NONODB_DAEMON")
    path = Path(configured) if configured else ROOT / "target" / "debug" / "nonodb-daemon"
    if not path.is_file():
        path = ROOT / "target" / "release" / "nonodb-daemon"
    if not path.is_file():
        raise SystemExit(f"missing daemon binary: {path}")
    return path


def wait_socket(path: Path, proc: subprocess.Popen, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited with {proc.returncode}")
        if path.exists():
            return
        time.sleep(0.05)
    raise RuntimeError("daemon socket did not appear")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="nonodb-ttl-strict-") as tmp_name:
        tmp = Path(tmp_name)
        sock = tmp / "nonodb.sock"
        mongo_port = free_port()
        env = os.environ.copy()
        env.update(
            {
                "NONODB_DATA": str(tmp / "data"),
                "NONODB_SOCK": str(sock),
                "NONODB_MAP_SIZE": "64MiB",
                "NONODB_TTL_SWEEP_INTERVAL": "3600",
                "NONODB_MONGO_ADDR": f"127.0.0.1:{mongo_port}",
                "RUST_LOG": "warn",
            }
        )
        log = (tmp / "daemon.log").open("wb")
        proc = subprocess.Popen(
            [str(daemon_binary())], env=env, stdout=log, stderr=subprocess.STDOUT
        )
        try:
            wait_socket(sock, proc)
            # Let the startup sweep run before inserting TTL documents.
            time.sleep(1.4)
            db = NonoDB(str(sock), timeout=30)
            c = db.collection(COLL)
            c.insert_many(
                [
                    {
                        "_id": "a-expired",
                        "title": "alpha",
                        "embedding": [1.0, 0.0],
                        "_ttl": 1,
                    },
                    {
                        "_id": "b-live",
                        "title": "alpha",
                        "embedding": [0.9, 0.1],
                        "_ttl": 60,
                    },
                    {
                        "_id": "c-live",
                        "title": "beta",
                        "embedding": [0.0, 1.0],
                    },
                ]
            )
            c.create_text_index("title")
            c.create_fuzzy_index("title")
            c.create_vector_index("embedding", 2, "cosine")

            # Cache this visible result. The cache entry must not outlive TTL.
            before = c.find({"_id": "a-expired"})
            assert [doc["_id"] for doc in before] == ["a-expired"], before
            cache_before = c.find({})
            assert [doc["_id"] for doc in cache_before] == ["a-expired", "b-live", "c-live"]

            # Open a cursor before expiry; its next page must not leak the doc.
            cursor = c.find_cursor({}, batch_size=1)
            first = cursor.next_batch()
            assert [doc["_id"] for doc in first] == ["a-expired"], first

            time.sleep(1.2)

            found = c.find({})
            assert [doc["_id"] for doc in found] == ["b-live", "c-live"], found
            cached = c.find({})
            assert [doc["_id"] for doc in cached] == ["b-live", "c-live"], cached
            assert c.find_one({"_id": "a-expired"}) is None
            assert c.count({}) == 2
            assert c.lookup("_id", ["a-expired", "b-live"]) == [
                None,
                c.find_one({"_id": "b-live"}),
            ]
            assert c.get_many(["a-expired", "b-live"]) == [
                c.find_one({"_id": "b-live"})
            ]
            grouped = c.aggregate([{"$group": {"_id": None, "n": {"$sum": 1}}}])
            assert grouped == [{"_id": None, "n": 2}], grouped
            sql_rows = db.sql(f"SELECT _id FROM {COLL} ORDER BY _id", [])
            assert [row["_id"] for row in sql_rows] == ["b-live", "c-live"], sql_rows

            search = c.search_all(mode="lexical", query="alpha", fields=["title"], limit=10)
            assert [doc["_id"] for doc in search] == ["b-live"], search
            fuzzy = c.search_all(mode="fuzzy", query="alpha", fields=["title"], limit=10)
            assert all(doc["_id"] != "a-expired" for doc in fuzzy), fuzzy
            vector = c.vector_search("embedding", [1.0, 0.0], limit=1)
            assert [doc["_id"] for doc in vector] == ["b-live"], vector
            hybrid = c.search_all(
                mode="hybrid",
                query="alpha",
                fields=["title"],
                vector=[1.0, 0.0],
                vector_field="embedding",
                limit=10,
            )
            assert all(doc["_id"] != "a-expired" for doc in hybrid), hybrid

            second = cursor.next_batch()
            assert all(doc["_id"] != "a-expired" for doc in second), second
            cursor.close()

            try:
                from pymongo import MongoClient
            except ImportError:
                MongoClient = None
            if MongoClient is not None:
                mongo = MongoClient(
                    f"mongodb://127.0.0.1:{mongo_port}/?directConnection=true",
                    serverSelectionTimeoutMS=5_000,
                )
                wire_db = mongo.get_database("default")
                wire_find = list(wire_db[COLL].find({}))
                assert all(doc["_id"] != "a-expired" for doc in wire_find), wire_find
                assert wire_db[COLL].count_documents({}) == 2
                wire_group = list(
                    wire_db[COLL].aggregate(
                        [{"$group": {"_id": None, "n": {"$sum": 1}}}]
                    )
                )
                assert wire_group == [{"_id": None, "n": 2}], wire_group
                mongo.close()

            db.close()
            print("OK: strict TTL read filtering before sweep")
            return 0
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            log.close()


if __name__ == "__main__":
    raise SystemExit(main())
