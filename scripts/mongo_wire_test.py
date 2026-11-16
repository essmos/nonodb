#!/usr/bin/env python3
"""Smoke-test the experimental MongoDB wire listener with PyMongo.

Start the daemon with NONODB_MONGO_ADDR=127.0.0.1:37017, then run:

    python3 scripts/mongo_wire_test.py
"""

import os
import sys

from bson import ObjectId
from pymongo import MongoClient, ReturnDocument


def main() -> int:
    url = os.environ.get("NONODB_MONGO_URL", "mongodb://127.0.0.1:37017/?directConnection=true")
    client = MongoClient(url, serverSelectionTimeoutMS=3000)
    db = client.mongo_wire_smoke
    coll = db[f"items_{os.getpid()}"]
    try:
        client.admin.command("ping")
        inserted = coll.insert_many(
            [{"i": i, "group": "smoke", "tags": ["a", "b"]} for i in range(10)]
        )
        assert len(inserted.inserted_ids) == 10
        assert all(isinstance(value, ObjectId) for value in inserted.inserted_ids)
        assert coll.count_documents({"group": "smoke"}) == 10
        assert coll.estimated_document_count() == 10
        assert sorted(coll.distinct("tags")) == ["a", "b"]

        updated = coll.update_one(
            {"_id": inserted.inserted_ids[0]}, {"$set": {"i": 100}}
        )
        assert updated.matched_count == 1
        assert updated.modified_count == 1
        assert coll.find_one({"_id": inserted.inserted_ids[0]})["i"] == 100
        assert coll.update_many({"group": "smoke"}, {"$inc": {"i": 1}}).modified_count == 10

        coll.insert_one({"_id": "fm", "n": 1})
        changed = coll.find_one_and_update(
            {"_id": "fm"}, {"$inc": {"n": 1}}, return_document=ReturnDocument.AFTER
        )
        assert changed["n"] == 2
        assert coll.find_one_and_delete({"_id": "fm"})["n"] == 2

        assert coll.create_index("group") == "group_1"
        index_names = [index["name"] for index in coll.list_indexes()]
        assert "group" in index_names
        assert db.command({"dropIndexes": coll.name, "index": "group"})["ok"] == 1.0

        assert coll.delete_one({"group": "smoke"}).deleted_count == 1
        assert coll.delete_many({"group": "smoke"}).deleted_count == 9
        rows = list(
            coll.aggregate(
                [{"$match": {}}, {"$group": {"_id": "$group", "n": {"$sum": 1}}}]
            )
        )
        assert rows == []
        print("mongo wire smoke test: ok")
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
