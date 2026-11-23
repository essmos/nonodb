#!/usr/bin/env python3
"""End-to-end test for scripts/nonodb-migrate.py.

Requires a source MongoDB and a running NonoDB daemon with a Unix socket:

    MONGODB_MIGRATE_URL=mongodb://127.0.0.1:27017/?directConnection=true \
    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/nonodb_migrate_test.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from bson import Binary, Decimal128, ObjectId
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402

MIGRATE = ROOT / "scripts" / "nonodb-migrate.py"


def main() -> int:
    source_url = os.environ.get("MONGODB_MIGRATE_URL") or os.environ.get(
        "MONGODB_CONFORMANCE_URL"
    )
    if not source_url:
        print("MONGODB_MIGRATE_URL is required", file=sys.stderr)
        return 2
    socket = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
    suffix = uuid.uuid4().hex[:10]
    source_db = f"migrate_source_{suffix}"
    target_db = f"migrate_target_{suffix}"
    client = MongoClient(source_url, serverSelectionTimeoutMS=5_000)
    db = client[source_db]
    admin = NonoDB(socket_path=socket)
    report_path = Path(tempfile.gettempdir()) / f"nonodb-migrate-{suffix}.json"
    try:
        client.admin.command("ping")
        db["users"].insert_many(
            [
                {
                    "_id": ObjectId("64b64c000000000000000001"),
                    "email": "a@example.test",
                    "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
                    "dec": Decimal128("1.50"),
                    "bin": Binary(b"\x00\x01"),
                    "tags": ["a", "b"],
                },
                {
                    "_id": ObjectId("64b64c000000000000000002"),
                    "email": "b@example.test",
                    "tags": ["b"],
                },
            ]
        )
        db["users"].create_index(
            [("email", 1)], unique=True, sparse=True, name="email_unique"
        )
        db["orders"].insert_many(
            [{"_id": str(i), "user": f"u{i % 3}", "total": i * 1.5} for i in range(10)]
        )
        db["orders"].create_index([("user", 1), ("total", -1)], name="user_total")
        db["orders"].create_index(
            [("total", 1)],
            name="positive_total",
            partialFilterExpression={"total": {"$gte": 0}},
        )
        db["sessions"].insert_one(
            {"_id": "s1", "createdAt": datetime.now(timezone.utc)}
        )
        db["sessions"].create_index(
            [("createdAt", 1)], expireAfterSeconds=120, name="ttl_created"
        )
        db["scratch"].insert_one({"_id": "excluded"})

        command = [
            sys.executable,
            str(MIGRATE),
            "--mongo-uri",
            source_url,
            "--source-db",
            source_db,
            "--socket",
            socket,
            "--database",
            target_db,
            "--exclude",
            "scratch",
            "--report",
            str(report_path),
        ]
        result = subprocess.run(command, text=True, capture_output=True)
        if result.returncode != 0:
            print(result.stdout, file=sys.stderr)
            print(result.stderr, file=sys.stderr)
            raise AssertionError("nonodb-migrate failed")
        report = json.loads(report_path.read_text())
        assert report["ok"] is True, report
        assert report["source"]["uri"].count("@") == 0, report["source"]
        collections = {entry["name"]: entry for entry in report["collections"]}
        assert set(collections) == {"users", "orders", "sessions"}, collections
        assert collections["users"]["count"] == 2
        assert collections["orders"]["count"] == 10
        assert collections["sessions"]["ttl_seconds"] == 120
        assert collections["sessions"]["ttl_field"] == "createdAt"
        assert all(entry["sha256"] for entry in collections.values())

        target = NonoDB(socket_path=socket, database=target_db)
        try:
            assert set(target.collections()) == {"users", "orders", "sessions"}
            assert target.collection("users").count() == 2
            assert target.collection("orders").count() == 10
            assert target.get_collection_ttl("sessions") == 120
            assert target.get_collection_ttl_field("sessions") == "createdAt"
            indexes = target.collection("users").list_indexes()
            assert any(
                index["unique"]
                and index["sparse"]
                and index["fields"] == ["email"]
                for index in indexes
            ), indexes
            order_indexes = target.collection("orders").list_indexes()
            assert any(
                index["fields"] == ["total"]
                and index.get("partial_filter_expression") == {"total": {"$gte": 0}}
                for index in order_indexes
            ), order_indexes
            assert target.collection("orders").find_one({"_id": "0"})["total"] == 0.0
            restored = target.collection("users").find_one(
                {"_id": {"$oid": "64b64c000000000000000001"}}
            )
            assert restored["dec"] == {"$numberDecimal": "1.50"}, restored
            assert restored["created_at"]["$date"]["$numberLong"] == "1704067200000", restored
            try:
                target.collection("users").insert(
                    {"_id": "duplicate", "email": "a@example.test"}
                )
            except Exception as error:  # noqa: BLE001 - driver error type varies
                assert "duplicate" in str(error).lower(), error
            else:
                raise AssertionError("unique index was not created")
        finally:
            target.close()

        dry_run = [
            *command[:-2],
            "--database",
            f"{target_db}_dry",
            "--dry-run",
        ]
        result = subprocess.run(dry_run, text=True, capture_output=True)
        assert result.returncode == 0, result.stderr
        assert "Validated 13 documents" in result.stdout, result.stdout
    finally:
        client.drop_database(source_db)
        client.close()
        try:
            admin.exec({"op": "drop_database", "name": target_db})
        except Exception:
            pass
        try:
            admin.exec({"op": "drop_database", "name": f"{target_db}_dry"})
        except Exception:
            pass
        admin.close()
        report_path.unlink(missing_ok=True)

    print("nonodb-migrate test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
