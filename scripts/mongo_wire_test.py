#!/usr/bin/env python3
"""Smoke-test the experimental MongoDB wire listener with PyMongo.

Start the daemon with NONODB_MONGO_ADDR=127.0.0.1:37017, then run:

    python3 scripts/mongo_wire_test.py
"""

import os
import re
import sys
import uuid
from urllib.parse import quote

from bson import Int64, ObjectId
from bson.regex import Regex
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import BulkWriteError, OperationFailure
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
        try:
            coll.insert_one({"_id": inserted.inserted_ids[0]})
        except OperationFailure as error:
            assert error.code == 11000, error
        else:
            raise AssertionError("duplicate _id was accepted")

        for ordered, expected_inserted, expected_ids in (
            (True, 1, ["batch-a", "batch-b"]),
            (False, 2, ["batch-a", "batch-b", "batch-c"]),
        ):
            batch = db[f"batch_{ordered}_{os.getpid()}"]
            batch.insert_one({"_id": "batch-a"})
            try:
                batch.insert_many(
                    [{"_id": "batch-b"}, {"_id": "batch-a"}, {"_id": "batch-c"}],
                    ordered=ordered,
                )
            except BulkWriteError as error:
                assert error.details["nInserted"] == expected_inserted, error.details
                assert error.details["writeErrors"][0]["index"] == 1, error.details
            else:
                raise AssertionError("duplicate batch _id was accepted")
            assert sorted(doc["_id"] for doc in batch.find({})) == expected_ids
        for label, command, expected_code in [
            (
                "namespace not found",
                {"listIndexes": f"missing_{uuid.uuid4().hex[:8]}"},
                26,
            ),
            (
                "cursor not found",
                {
                    "getMore": Int64(9_223_372_036_854_775_000),
                    "collection": coll.name,
                },
                43,
            ),
            (
                "unknown command",
                {"definitelyUnknownCommand": 1},
                59,
            ),
        ]:
            try:
                db.command(command)
            except OperationFailure as error:
                assert error.code == expected_code, (label, error)
            else:
                raise AssertionError(f"{label} was accepted")
        assert coll.count_documents({"group": "smoke"}) == 10
        assert coll.estimated_document_count() == 10
        assert db.command(
            {
                "count": coll.name,
                "query": {"group": "smoke"},
                "skip": 3,
                "limit": 4,
            }
        )["n"] == 4
        assert db.command(
            {"count": coll.name, "query": {"group": "smoke"}, "skip": 8}
        )["n"] == 2
        assert sorted(coll.distinct("tags")) == ["a", "b"]
        assert sorted(
            db.command(
                {
                    "distinct": coll.name,
                    "key": "tags",
                    "maxTimeMS": 5000,
                }
            )["values"]
        ) == ["a", "b"]
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

        upsert_coll = db[f"upsert_smoke_{os.getpid()}"]
        generated = upsert_coll.update_one(
            {"key": "generated"}, {"$set": {"n": 1}}, upsert=True
        )
        assert isinstance(generated.upserted_id, ObjectId), generated.upserted_id
        generated_doc = upsert_coll.find_one({"key": "generated"})
        assert generated_doc["_id"] == generated.upserted_id
        explicit = upsert_coll.update_one(
            {"key": "explicit"},
            {"$setOnInsert": {"_id": "kept"}, "$set": {"n": 2}},
            upsert=True,
        )
        assert explicit.upserted_id == "kept"
        asserted = upsert_coll.find_one_and_update(
            {"key": "fam"},
            {"$set": {"n": 3}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        assert isinstance(asserted["_id"], ObjectId), asserted["_id"]
        pipeline = upsert_coll.update_one(
            {"key": "pipeline"}, [{"$set": {"n": 4}}], upsert=True
        )
        assert isinstance(pipeline.upserted_id, ObjectId), pipeline.upserted_id
        replacement = upsert_coll.replace_one(
            {"key": "replacement"}, {"n": 5}, upsert=True
        )
        assert isinstance(replacement.upserted_id, ObjectId), replacement.upserted_id

        regex_coll = db[f"regex_smoke_{os.getpid()}"]
        regex_coll.insert_many([{"name": "abc"}, {"name": "xyz"}])
        assert regex_coll.find_one({"name": re.compile("^a")})["name"] == "abc"
        assert regex_coll.find_one({"name": {"$regex": re.compile("^a")}})[
            "name"
        ] == "abc"
        assert regex_coll.count_documents({"name": re.compile("^a")}) == 1
        assert [
            doc["name"]
            for doc in regex_coll.find({"name": {"$in": [re.compile("^x")]}})
        ] == ["xyz"]
        assert [
            doc["name"]
            for doc in regex_coll.aggregate(
                [{"$match": {"name": re.compile("^a")}}]
            )
        ] == ["abc"]
        assert [
            item["name"]
            for item in db.list_collections(filter={"name": re.compile("^regex_smoke_")})
        ] == [regex_coll.name]

        # BSON regex values are storable data. A direct regex literal remains a
        # string predicate; $eq compares the stored pattern.
        regex_coll.insert_one({"_id": "stored", "name": re.compile("^a", re.I)})
        stored = regex_coll.find_one({"_id": "stored"})["name"]
        assert isinstance(stored, Regex), stored
        assert stored.pattern == "^a" and stored.flags & re.I, stored
        assert regex_coll.find_one(
            {"name": {"$eq": re.compile("^a", re.I)}}
        )["_id"] == "stored"
        assert [doc["_id"] for doc in regex_coll.find({"name": {"$type": "regex"}})] == [
            "stored"
        ]
        regex_coll.update_one(
            {"_id": "stored"}, {"$set": {"name": re.compile("^b", re.M)}}
        )
        updated_regex = regex_coll.find_one({"_id": "stored"})["name"]
        assert updated_regex.pattern == "^b" and updated_regex.flags & re.M, updated_regex
        assert regex_coll.find_one({"name": {"$regex": "^b"}}) is None

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
        assert "group_1" in index_names
        custom_name = "custom_group_n"
        assert (
            coll.create_index([("group", 1), ("i", -1)], name=custom_name)
            == custom_name
        )
        custom = next(
            index for index in coll.list_indexes() if index["name"] == custom_name
        )
        assert list(custom["key"].items()) == [("group", 1), ("i", -1)]
        assert (
            db.command(
                {"dropIndexes": coll.name, "index": {"group": 1, "i": -1}}
            )["ok"]
            == 1.0
        )
        assert custom_name not in [
            index["name"] for index in coll.list_indexes()
        ]
        assert db.command({"dropIndexes": coll.name, "index": "group_1"})["ok"] == 1.0

        text_coll = db[f"text_smoke_{uuid.uuid4().hex[:8]}"]
        text_coll.insert_many(
            [
                {"n": 1, "title": "Alpha beta", "body": "gamma delta"},
                {"n": 2, "title": "Alpha", "body": "beta gamma"},
                {"n": 3, "title": "Delta", "body": "epsilon"},
                {"n": 4, "title": "alpha beta gamma", "body": "delta"},
            ]
        )
        try:
            text_coll.create_index(
                [("title", "text"), ("body", "text")],
                default_language="none",
            )
        except OperationFailure as error:
            assert error.code == 2, error
        else:
            raise AssertionError("multi-field text index was accepted")
        try:
            text_coll.create_index(
                [("title", "text")],
                weights={"title": 2},
                default_language="none",
            )
        except OperationFailure as error:
            assert error.code == 2, error
        else:
            raise AssertionError("non-default text weight was accepted")
        text_name = text_coll.create_index(
            [("title", "text")],
            default_language="none",
        )
        assert text_name == "title_text"
        text_index = next(
            index for index in text_coll.list_indexes() if index["name"] == text_name
        )
        assert list(text_index["key"].items()) == [("title", "text")]
        assert text_index["default_language"] == "none"
        assert "weights" not in text_index
        assert sorted(
            doc["n"]
            for doc in text_coll.find(
                {"$text": {"$search": '"alpha beta"'}}, {"n": 1, "_id": 0}
            )
        ) == [1, 4]
        assert sorted(
            doc["n"]
            for doc in text_coll.find(
                {"$text": {"$search": "alpha gamma"}}, {"n": 1, "_id": 0}
            )
        ) == [4]
        assert sorted(
            doc["n"]
            for doc in text_coll.find(
                {"$text": {"$search": "alpha -beta"}}, {"n": 1, "_id": 0}
            )
        ) == [2]
        scored = list(
            text_coll.find(
                {"$text": {"$search": "alpha beta"}},
                {"n": 1, "_id": 0, "score": {"$meta": "textScore"}},
                sort=[("score", {"$meta": "textScore"})],
            )
        )
        assert sorted(doc["n"] for doc in scored) == [1, 4]
        scores = [doc["score"] for doc in scored]
        assert scores == sorted(scores, reverse=True)
        assert all(score >= 0 for score in scores)
        assert (
            db.command(
                {
                    "count": text_coll.name,
                    "query": {
                        "$text": {"$search": "alpha"},
                        "n": {"$gte": 2},
                    },
                }
            )["n"]
            == 2
        )
        assert sorted(
            db.command(
                {
                    "distinct": text_coll.name,
                    "key": "n",
                    "query": {"$text": {"$search": "alpha"}},
                }
            )["values"]
        ) == [1, 2, 4]
        try:
            list(
                text_coll.find(
                    {"$text": {"$search": "alpha"}}, max_time_ms=1000
                )
            )
        except OperationFailure as error:
            assert error.code == 2, error
        else:
            raise AssertionError("maxTimeMS with $text was accepted")
        assert sorted(
            doc["n"]
            for doc in text_coll.aggregate(
                [
                    {
                        "$match": {
                            "$text": {"$search": "alpha"},
                            "n": {"$gte": 2},
                        }
                    },
                    {"$project": {"_id": 0, "n": 1}},
                ]
            )
        ) == [2, 4]
        try:
            list(
                text_coll.aggregate(
                    [
                        {"$match": {"n": {"$gte": 1}}},
                        {"$match": {"$text": {"$search": "alpha"}}},
                    ]
                )
            )
        except OperationFailure as error:
            assert error.code == 2, error
        else:
            raise AssertionError("non-leading aggregate $text was accepted")
        assert (
            db.command(
                {
                    "dropIndexes": text_coll.name,
                    "index": {"title": "text"},
                }
            )["ok"]
            == 1.0
        )
        assert text_name not in [index["name"] for index in text_coll.list_indexes()]
        text_coll.drop()

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
