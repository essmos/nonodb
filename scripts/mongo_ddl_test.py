#!/usr/bin/env python3
"""Test MongoDB DDL options translated by the NonoDB wire listener.

The listener must accept the two common migration options:

- ``expireAfterSeconds`` on a createIndexes entry becomes the collection TTL;
- ``sparse`` is accepted for a unique index (NonoDB unique indexes already
  behave like ``unique + sparse``).

Run the daemon with a MongoDB listener and a Unix socket, then:

    NONODB_MONGO_URL=... NONODB_SOCK=... python3 scripts/mongo_ddl_test.py
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402


def main() -> int:
    url = os.environ.get(
        "NONODB_MONGO_URL", "mongodb://127.0.0.1:37017/?directConnection=true"
    )
    socket = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
    database = os.environ.get("NONODB_MONGO_DB", "mongo_ddl_test")
    client = MongoClient(url, serverSelectionTimeoutMS=3_000)
    db = client[database]
    nd = NonoDB(socket_path=socket, database=database)
    suffix = uuid.uuid4().hex[:8]

    ttl_name = f"ttl_index_{suffix}"
    sparse_name = f"sparse_index_{suffix}"
    try:
        client.admin.command("ping")

        ttl = db[ttl_name]
        assert (
            ttl.create_index(
                [("createdAt", 1)], expireAfterSeconds=42, name="created_ttl"
            )
            == "created_ttl"
        )
        assert nd.get_collection_ttl(ttl_name) == 42

        sparse = db[sparse_name]
        assert (
            sparse.create_index(
                [("email", 1)], unique=True, sparse=True, name="email_unique"
            )
            == "email_unique"
        )
        sparse.insert_many(
            [
                {"_id": "missing-a"},
                {"_id": "missing-b"},
                {"_id": "null", "email": None},
            ]
        )
        try:
            sparse.insert_one({"_id": "duplicate", "email": "same@example.test"})
            sparse.insert_one({"_id": "duplicate-2", "email": "same@example.test"})
        except DuplicateKeyError as error:
            assert error.code == 11000, error
        else:
            raise AssertionError("unique + sparse index accepted a duplicate")

        multi = {
            "createIndexes": f"multi_ttl_{suffix}",
            "indexes": [
                {"key": {"a": 1}, "expireAfterSeconds": 1},
                {"key": {"b": 1}, "expireAfterSeconds": 2},
            ],
        }
        try:
            db.command(multi)
        except OperationFailure as error:
            assert error.code == 2, error
        else:
            raise AssertionError("multiple TTL indexes were accepted")

        compound = {
            "createIndexes": f"compound_ttl_{suffix}",
            "indexes": [{"key": {"a": 1, "b": 1}, "expireAfterSeconds": 1}],
        }
        try:
            db.command(compound)
        except OperationFailure as error:
            assert error.code == 2, error
        else:
            raise AssertionError("compound TTL index was accepted")

        zero = {
            "createIndexes": f"zero_ttl_{suffix}",
            "indexes": [{"key": {"a": 1}, "expireAfterSeconds": 0}],
        }
        try:
            db.command(zero)
        except OperationFailure as error:
            assert error.code == 2, error
        else:
            raise AssertionError("expireAfterSeconds: 0 was accepted")
    finally:
        for name in (ttl_name, sparse_name):
            db.drop_collection(name)
        client.close()

    print("mongo DDL translation test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
