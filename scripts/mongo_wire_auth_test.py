#!/usr/bin/env python3
"""Test MongoDB wire SCRAM authentication and authorization error codes.

Starts an isolated daemon with an auth-file principal and verifies that
in-scope operations work while out-of-scope reads, writes, and database
administration fail with MongoDB code 13.
"""

import json
import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

from pymongo import MongoClient
from pymongo.errors import OperationFailure

ROOT = Path(__file__).resolve().parents[1]
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
USERNAME = "app"
PASSWORD = "app-secret"
AUTH_SOURCE = "admin"
DATABASE = "appdb"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_tcp(port: int, proc: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited with {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("daemon did not start listening")


def auth_document() -> dict:
    return {
        "version": 1,
        "principals": {
            "app": {
                "scopes": [
                    f"read:{DATABASE}/orders",
                    f"write:{DATABASE}/orders",
                    f"admin:{DATABASE}/orders",
                ]
            }
        },
        "tokens": [],
        "certificates": [],
    }


def expect_code(operation, expected: int, label: str) -> None:
    try:
        operation()
    except OperationFailure as error:
        assert error.code == expected, (label, error.code, error)
    else:
        raise AssertionError(f"{label} was accepted")


def main() -> int:
    if not DAEMON.exists():
        raise SystemExit(f"daemon binary not found: {DAEMON}")
    with tempfile.TemporaryDirectory(prefix="nonodb-mongo-auth-") as temporary:
        temp = Path(temporary)
        auth_file = temp / "auth.json"
        auth_file.write_text(json.dumps(auth_document(), indent=2) + "\n")
        port = free_port()
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(temp / "data"),
            NONODB_SOCK=str(temp / "nonodb.sock"),
            NONODB_MAP_SIZE="64MiB",
            NONODB_AUTH_FILE=str(auth_file),
            NONODB_MONGO_ADDR=f"127.0.0.1:{port}",
            NONODB_MONGO_USERNAME=USERNAME,
            NONODB_MONGO_PASSWORD=PASSWORD,
            NONODB_MONGO_PRINCIPAL="app",
            NONODB_MONGO_AUTH_SOURCE=AUTH_SOURCE,
            RUST_LOG="warn",
        )
        log = (temp / "daemon.log").open("wb")
        proc = subprocess.Popen(
            [str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT
        )
        credentials = f"{quote(USERNAME, safe='')}:{quote(PASSWORD, safe='')}"
        url = (
            f"mongodb://{credentials}@127.0.0.1:{port}/"
            f"?directConnection=true&authSource={AUTH_SOURCE}"
        )
        client = MongoClient(url, serverSelectionTimeoutMS=5000)
        try:
            wait_tcp(port, proc)
            assert client.admin.command("ping")["ok"] == 1.0
            db = client[DATABASE]
            orders = db.orders
            inserted = orders.insert_one({"state": "new"})
            assert orders.find_one({"_id": inserted.inserted_id})["state"] == "new"
            assert "orders" in db.list_collection_names()
            assert DATABASE in client.list_database_names()

            expect_code(
                lambda: db.customers.find_one({}),
                13,
                "out-of-scope read",
            )
            expect_code(
                lambda: db.customers.insert_one({"blocked": True}),
                13,
                "out-of-scope write",
            )
            expect_code(
                lambda: client["otherdb"].orders.insert_one({"blocked": True}),
                13,
                "out-of-scope database write",
            )
            expect_code(
                lambda: db.command({"dropDatabase": 1}),
                13,
                "out-of-scope dropDatabase",
            )
            expect_code(
                lambda: db.command(
                    {"createIndexes": "customers", "indexes": [{"key": {"x": 1}}]}
                ),
                13,
                "out-of-scope index creation",
            )
        finally:
            client.close()
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
            log.close()

    print("mongo wire auth test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
