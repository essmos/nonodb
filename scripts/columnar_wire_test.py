#!/usr/bin/env python3
"""Exercise columnar collections through the MongoDB wire listener.

Requires a running NonoDB MongoDB listener:

    NONODB_MONGO_URL='mongodb://127.0.0.1:37017/?directConnection=true' \
      python3 scripts/columnar_wire_test.py
"""

from __future__ import annotations

import os
import sys
import uuid

from pymongo import MongoClient
from pymongo.errors import OperationFailure


def main() -> int:
    url = os.environ.get("NONODB_MONGO_URL")
    if not url:
        print("NONODB_MONGO_URL is required", file=sys.stderr)
        return 2
    client = MongoClient(url)
    database = f"columnar_wire_{uuid.uuid4().hex}"
    db = client[database]
    try:
        db.create_collection(
            "events",
            columnar={
                "bucketMaxDocs": 64,
                "bucketMaxBytes": 65536,
                "compression": "max",
            },
        )
        info = next(iter(db.list_collections(filter={"name": "events"})))
        options = info["options"]["columnar"]
        assert options["bucketMaxDocs"] == 64, options
        assert options["bucketMaxBytes"] == 65536, options
        assert options["compression"] == "max", options

        events = db.events
        events.insert_many(
            [
                {"_id": "a", "g": "x", "v": 1},
                {"_id": "b", "g": "x", "v": 2},
                {"_id": "c", "g": "y", "v": 3},
            ]
        )
        assert events.count_documents({}) == 3
        assert events.find_one({"_id": "b"})["v"] == 2
        assert events.update_one({"g": "x"}, {"$inc": {"v": 10}}).modified_count == 1
        assert events.delete_one({"g": "y"}).deleted_count == 1
        assert events.count_documents({}) == 2
        grouped = list(
            events.aggregate(
                [{"$group": {"_id": "$g", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
            )
        )
        assert grouped == [{"_id": "x", "n": 2}], grouped
        events.create_index("v", unique=True)
        assert events.find_one({"v": 11})["_id"] == "a"

        for bad in (
            {"columnar": {"bucketMaxDocs": 0}},
            {"columnar": {"compression": "turbo"}},
            {"columnar": {"unsupported": 1}},
            {
                "timeseries": {"timeField": "ts"},
                "columnar": {"bucketMaxDocs": 64},
            },
        ):
            try:
                db.create_collection(f"bad_{uuid.uuid4().hex}", **bad)
            except OperationFailure:
                pass
            else:
                raise AssertionError(f"expected create failure for {bad}")

        print("columnar MongoDB wire test: ok")
        return 0
    finally:
        client.drop_database(database)
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
