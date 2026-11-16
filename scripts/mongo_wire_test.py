#!/usr/bin/env python3
"""Smoke-test the experimental MongoDB wire listener with PyMongo.

Start the daemon with NONODB_MONGO_ADDR=127.0.0.1:37017, then run:

    python3 scripts/mongo_wire_test.py
"""

import os
import sys
import uuid
from urllib.parse import quote

from bson import ObjectId
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern


def main() -> int:
    url = os.environ.get(
        "NONODB_MONGO_URL", "mongodb://127.0.0.1:37017/?directConnection=true"
    )
    username = os.environ.get("NONODB_MONGO_USERNAME")
    password = os.environ.get("NONODB_MONGO_PASSWORD")
    if username and password and "@" not in url.split("://", 1)[-1]:
        credentials = f"{quote(username, safe='')}:{quote(password, safe='')}@"
        url = url.replace("://", f"://{credentials}", 1)
        auth_source = os.environ.get("NONODB_MONGO_AUTH_SOURCE", "admin")
        url += ("&" if "?" in url else "?") + f"authSource={auth_source}"
    if os.environ.get("NONODB_MONGO_TLS", "").lower() in ("1", "true", "yes") and "tls=" not in url:
        url += "&tls=true&tlsAllowInvalidCertificates=true"
    options = {"serverSelectionTimeoutMS": 3000}
    mechanism = os.environ.get("NONODB_MONGO_AUTH_MECHANISM")
    if mechanism:
        options["authMechanism"] = mechanism
    client = MongoClient(url, **options)
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
        streamed = list(
            coll.aggregate(
                [{"$match": {"group": "smoke"}}, {"$project": {"i": 1}}],
                batchSize=3,
            )
        )
        assert len(streamed) == 10
        grouped = list(
            coll.aggregate(
                [{"$group": {"_id": "$group", "n": {"$sum": 1}}}],
                batchSize=1,
            )
        )
        assert grouped == [{"_id": "smoke", "n": 10}]
        assert [
            item["name"]
            for item in db.list_collections(filter={"name": coll.name})
        ] == [coll.name]
        assert [
            item["name"]
            for item in client.list_databases(
                filter={"name": db.name}, nameOnly=True
            )
        ] == [db.name]

        for label, operation in [
            ("collation", lambda: list(coll.find({}, collation={"locale": "en"}))),
            (
                "bypassDocumentValidation",
                lambda: coll.insert_one(
                    {"unsupported": True}, bypass_document_validation=True
                ),
            ),
            (
                "aggregate cursor option",
                lambda: db.command(
                    {
                        "aggregate": coll.name,
                        "pipeline": [],
                        "cursor": {"collation": {}},
                    }
                ),
            ),
            (
                "writeConcern majority",
                lambda: coll.with_options(
                    write_concern=WriteConcern("majority")
                ).insert_one({"unsupported": True}),
            ),
            (
                "readConcern snapshot",
                lambda: coll.with_options(
                    read_concern=ReadConcern("snapshot")
                ).find_one({}),
            ),
        ]:
            try:
                operation()
            except OperationFailure as error:
                assert error.code == 2, (label, error)
            else:
                raise AssertionError(f"unsupported option was accepted: {label}")

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

        sort_coll = db[f"sort_smoke_{os.getpid()}"]
        sort_coll.insert_many([{"n": i, "m": i % 2} for i in range(7)])
        assert [doc["n"] for doc in sort_coll.find({}, sort=[("n", -1)], batch_size=2)] == [
            6,
            5,
            4,
            3,
            2,
            1,
            0,
        ]
        assert [
            doc["n"]
            for doc in sort_coll.find(
                {}, sort=[("m", 1), ("n", -1)], skip=1, limit=4, batch_size=2
            )
        ] == [4, 2, 0, 5]
        sort_coll.drop()

        ddl_name = f"ddl_smoke_{uuid.uuid4().hex[:8]}"
        ddl = db.create_collection(ddl_name)
        assert ddl_name in db.list_collection_names()
        ddl.insert_one({"ddl": True})
        assert ddl.count_documents({}) == 1
        ddl.drop()
        assert ddl_name not in db.list_collection_names()
        assert db.name in client.list_database_names()

        dropdb_name = f"dropdb_smoke_{uuid.uuid4().hex[:8]}"
        dropdb = client[dropdb_name]
        dropdb.items.insert_one({"drop": True})
        assert dropdb_name in client.list_database_names()
        client.drop_database(dropdb_name)
        assert dropdb_name not in client.list_database_names()

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
