#!/usr/bin/env python3
"""Compare the NonoDB MongoDB wire listener with a real MongoDB server.

Both endpoints must be reachable without authentication. The script creates a
unique database on each side, runs the same PyMongo operations, and compares
normalized results and error codes.

    NONODB_MONGO_URL='mongodb://127.0.0.1:37017/?directConnection=true' \
    MONGODB_CONFORMANCE_URL='mongodb://127.0.0.1:27017/?directConnection=true' \
    python3 scripts/mongo_conformance_test.py

Set `MONGODB_CONFORMANCE_KEEP=1` to keep the temporary databases for debugging.
"""

import os
import re
import sys
import uuid
from datetime import datetime, timezone

from bson import (
    Binary,
    Decimal128,
    Int64,
    MaxKey,
    MinKey,
    ObjectId,
    Timestamp,
    json_util,
)
from pymongo import MongoClient
from pymongo.errors import OperationFailure


OBJECT_ID = ObjectId("64b64c000000000000000001")
DOCS = [
    {
        "_id": "a",
        "n": 1,
        "tag": "x",
        "nested": {"v": 2},
        "arr": [1, 2, 3],
        "items": [{"k": "a", "q": 2}, {"k": "b", "q": 5}],
        "when": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "flag": True,
        "nil": None,
        "dec": Decimal128("1.50"),
        "bin": Binary(b"\x00\x01\xff"),
        "ts": Timestamp(1_700_000_000, 7),
        "low": MinKey(),
        "high": MaxKey(),
        "oid": OBJECT_ID,
    },
    {
        "_id": "b",
        "n": 2,
        "tag": "x",
        "nested": {"v": 1},
        "arr": [2, 3, 4],
        "items": [{"k": "a", "q": 1}],
        "flag": False,
        "dec": Decimal128("-0.25"),
    },
    {
        "_id": "c",
        "n": 3,
        "tag": "y",
        "nested": {"v": 2},
        "arr": [1, 3, 5],
        "items": [{"k": "c", "q": 4}],
        "flag": True,
        "dec": Decimal128("NaN"),
    },
]


def canonical(value):
    """Canonicalize BSON values without depending on field insertion order."""
    if isinstance(value, dict):
        return {key: canonical(item) for key, item in value.items()}
    if isinstance(value, list):
        return [canonical(item) for item in value]
    if isinstance(value, tuple):
        return [canonical(item) for item in value]
    encoded = json_util.dumps(
        value, json_options=json_util.RELAXED_JSON_OPTIONS
    )
    decoded = json_util.loads(encoded)
    if isinstance(decoded, dict):
        return {key: canonical(decoded[key]) for key in sorted(decoded)}
    if isinstance(decoded, list):
        return [canonical(item) for item in decoded]
    return decoded


def check(name, function, nonodb_db, mongodb_db):
    try:
        expected = canonical(function(nonodb_db))
    except Exception as error:  # noqa: BLE001 - report both sides explicitly
        expected = f"{type(error).__name__}: {error}"
    try:
        actual = canonical(function(mongodb_db))
    except Exception as error:  # noqa: BLE001
        actual = f"{type(error).__name__}: {error}"
    if expected != actual:
        print(f"FAIL {name}")
        print(f"  NonoDB:  {expected!r}")
        print(f"  MongoDB: {actual!r}")
        return False
    print(f"ok   {name}")
    return True


TEXT_DOCS = [
    {"_id": "t1", "n": 1, "title": "Alpha beta", "body": "gamma delta"},
    {"_id": "t2", "n": 2, "title": "Alpha", "body": "beta gamma"},
    {"_id": "t3", "n": 3, "title": "Delta", "body": "epsilon"},
    {"_id": "t4", "n": 4, "title": "alpha beta gamma", "body": "delta"},
]


def text_ids(db, query):
    return sorted(
        doc["_id"]
        for doc in db.text_items.find(
            {"$text": {"$search": query}}, {"_id": 1}
        )
    )


def create_text_index(db):
    # NonoDB supports one single-field text index. Keep this comparison on a
    # query mix whose result does not depend on MongoDB's OR semantics.
    return db.text_items.create_index(
        [("title", "text")],
        default_language="none",
    )


def command_error_code(function):
    def run(db):
        try:
            function(db)
        except OperationFailure as error:
            return {"code": error.code}
        return {"code": "no error"}

    return run


def sorted_docs(db, filter=None, **kwargs):
    return list(db.items.find(filter or {}).sort([("_id", 1)], **kwargs))


def unique_missing_outcomes(db):
    """Plain unique indexes treat a missing field as null."""
    coll = db.unique_missing
    coll.create_index([("email", 1)], unique=True)
    outcomes = []
    for document in (
        {"_id": "missing-a"},
        {"_id": "missing-b"},
        {"_id": "explicit-null", "email": None},
    ):
        try:
            coll.insert_one(document)
            outcomes.append("ok")
        except OperationFailure as error:
            outcomes.append(error.code)
    return outcomes


def index_signature(db):
    return sorted(
        (
            tuple(index["key"].items()),
            index["name"],
            index.get("unique", False),
        )
        for index in db.items.list_indexes()
    )


def create_custom_index(db):
    name = db.items.create_index(
        [("tag", -1), ("n", 1)], name="custom_tag_n_index"
    )
    return {"created": name, "indexes": index_signature(db)}


def update_result(result):
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "upserted": result.upserted_id,
    }


def delete_result(result):
    return {"deleted": result.deleted_count}


def ping_ok(db):
    return {"ok": db.command("ping")["ok"]}


def create_index(db):
    db.items.create_index([("tag", 1), ("n", -1)])
    return index_signature(db)


def drop_secondary_index(db):
    name = next(
        index["name"] for index in db.items.list_indexes() if index["name"] != "_id_"
    )
    db.items.drop_index(name)
    return index_signature(db)


def main() -> int:
    nonodb_url = os.environ.get(
        "NONODB_MONGO_URL", "mongodb://127.0.0.1:37017/?directConnection=true"
    )
    mongodb_url = os.environ.get("MONGODB_CONFORMANCE_URL")
    if not mongodb_url:
        print("MONGODB_CONFORMANCE_URL is required", file=sys.stderr)
        return 2
    suffix = uuid.uuid4().hex[:10]
    nonodb_name = f"nonodb_conformance_{suffix}"
    mongodb_name = f"mongodb_conformance_{suffix}"
    nonodb = MongoClient(nonodb_url, serverSelectionTimeoutMS=5000)
    mongodb = MongoClient(mongodb_url, serverSelectionTimeoutMS=5000)
    nonodb_db = nonodb[nonodb_name]
    mongodb_db = mongodb[mongodb_name]
    failures = 0

    def check_both(name, function):
        nonlocal failures
        if not check(name, function, nonodb_db, mongodb_db):
            failures += 1

    try:
        check_both("ping", ping_ok)
        check_both(
            "insert many with BSON values",
            lambda db: db.items.insert_many(DOCS).inserted_ids,
        )
        check_both("find all", lambda db: sorted_docs(db))
        check_both(
            "sort missing/null ascending",
            lambda db: [doc["_id"] for doc in db.items.find({}).sort("nil", 1)],
        )
        check_both(
            "sort missing/null descending",
            lambda db: [doc["_id"] for doc in db.items.find({}).sort("nil", -1)],
        )
        check_both(
            "$in on _id deduplicates and keeps matches",
            lambda db: sorted(
                doc["_id"] for doc in db.items.find({"_id": {"$in": ["a", "c", "a"]}})
            ),
        )
        check_both(
            "$in on an indexed field",
            lambda db: sorted(doc["_id"] for doc in db.items.find({"n": {"$in": [1, 3]}})),
        )

        check_both(
            "$size single expression",
            lambda db: [
                doc["k"]
                for doc in db.items.aggregate(
                    [
                        {"$match": {"_id": "a"}},
                        {"$project": {"_id": 0, "k": {"$size": "$arr"}}},
                    ]
                )
            ],
        )
        check_both(
            "$size one-element array form",
            lambda db: [
                doc["k"]
                for doc in db.items.aggregate(
                    [
                        {"$match": {"_id": "a"}},
                        {"$project": {"_id": 0, "k": {"$size": ["$arr"]}}},
                    ]
                )
            ],
        )

        check_both(
            "insert text documents",
            lambda db: db.text_items.insert_many(TEXT_DOCS).inserted_ids,
        )
        check_both("create text index", create_text_index)
        check_both(
            "text phrases",
            lambda db: text_ids(db, '"alpha beta"'),
        )
        check_both("text single term", lambda db: text_ids(db, "alpha"))
        check_both("text negation", lambda db: text_ids(db, "delta -epsilon"))
        check_both(
            "text count with residual filter",
            lambda db: db.command(
                {
                    "count": "text_items",
                    "query": {
                        "$text": {"$search": "alpha"},
                        "n": {"$gte": 2},
                    },
                }
            )["n"],
        )
        check_both(
            "text distinct with residual filter",
            lambda db: sorted(
                db.command(
                    {
                        "distinct": "text_items",
                        "key": "n",
                        "query": {"$text": {"$search": "alpha"}},
                    }
                )["values"]
            ),
        )
        check_both(
            "text leading aggregate match",
            lambda db: sorted(
                doc["n"]
                for doc in db.text_items.aggregate(
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
            ),
        )
        check_both(
            "duplicate _id error",
            command_error_code(lambda db: db.items.insert_one({"_id": "a"})),
        )
        check_both(
            "range filter",
            lambda db: sorted_docs(db, {"n": {"$gte": 2}}),
        )
        check_both(
            "array all filter",
            lambda db: sorted_docs(db, {"arr": {"$all": [1, 3]}}),
        )
        check_both(
            "elemMatch filter",
            lambda db: sorted_docs(db, {"items": {"$elemMatch": {"q": {"$gt": 3}}}}),
        )
        check_both(
            "dotted path filter",
            lambda db: sorted_docs(db, {"nested.v": 2}),
        )
        check_both(
            "regex filter",
            lambda db: sorted_docs(db, {"tag": {"$regex": "^x"}}),
        )
        check_both(
            "BSON regex value filter",
            lambda db: sorted_docs(db, {"tag": re.compile("^x")}),
        )
        check_both(
            "BSON regex value in $in",
            lambda db: sorted_docs(db, {"tag": {"$in": [re.compile("^y")]}}),
        )
        check_both(
            "projection and array slice",
            lambda db: db.items.find_one(
                {"_id": "a"},
                {"_id": 0, "n": 1, "nested.v": 1, "arr": {"$slice": 2}},
            ),
        )
        check_both(
            "sort skip limit",
            lambda db: list(
                db.items.find({}, {"_id": 1, "n": 1})
                .sort([("n", -1)])
                .skip(1)
                .limit(1)
            ),
        )
        check_both("count", lambda db: db.items.count_documents({"tag": "x"}))
        check_both(
            "count command skip and limit",
            lambda db: db.command(
                {
                    "count": "items",
                    "query": {"n": {"$gte": 1}},
                    "skip": 1,
                    "limit": 2,
                }
            )["n"],
        )
        check_both("distinct", lambda db: sorted(db.items.distinct("tag")))
        check_both(
            "distinct command maxTimeMS",
            lambda db: sorted(
                db.command(
                    {"distinct": "items", "key": "tag", "maxTimeMS": 5000}
                )["values"]
            ),
        )
        check_both(
            "update set inc push",
            lambda db: update_result(
                db.items.update_one(
                    {"_id": "a"},
                    {
                        "$set": {"status": "new"},
                        "$inc": {"n": 10},
                        "$push": {"arr": 4},
                    },
                )
            ),
        )
        check_both("find after update", lambda db: sorted_docs(db))
        check_both(
            "upsert",
            lambda db: update_result(
                db.items.update_one(
                    {"_id": "d"}, {"$set": {"n": 4, "tag": "w"}}, upsert=True
                )
            ),
        )
        check_both("find after upsert", lambda db: sorted_docs(db))
        check_both(
            "aggregate group and sort",
            lambda db: list(
                db.items.aggregate(
                    [
                        {"$group": {"_id": "$tag", "total": {"$sum": "$n"}}},
                        {"$sort": {"_id": 1}},
                    ],
                    batchSize=1,
                )
            ),
        )
        check_both(
            "aggregate unwind and project",
            lambda db: list(
                db.items.aggregate(
                    [
                        {"$match": {"tag": "x"}},
                        {"$unwind": "$items"},
                        {"$project": {"_id": 1, "k": "$items.k", "q": "$items.q"}},
                        {"$sort": {"_id": 1, "k": 1}},
                    ],
                    batchSize=1,
                )
            ),
        )
        check_both("create index and list", create_index)
        check_both("drop secondary index", drop_secondary_index)
        check_both("custom index name and direction", create_custom_index)
        check_both("plain unique missing field", unique_missing_outcomes)
        check_both("list collection names", lambda db: sorted(db.list_collection_names()))
        check_both(
            "delete one",
            lambda db: delete_result(db.items.delete_one({"_id": "b"})),
        )
        check_both(
            "delete many",
            lambda db: delete_result(db.items.delete_many({"n": {"$gte": 4}})),
        )
        check_both("find after deletes", lambda db: sorted_docs(db))
        check_both(
            "namespace not found error",
            command_error_code(
                lambda db: db.command({"listIndexes": "missing_collection"})
            ),
        )
        check_both(
            "cursor not found error",
            command_error_code(
                lambda db: db.command(
                    {
                        "getMore": Int64(9_223_372_036_854_775_000),
                        "collection": "items",
                    }
                )
            ),
        )
        check_both(
            "invalid collation error",
            command_error_code(
                lambda db: db.command(
                    {
                        "find": "items",
                        "filter": {},
                        "collation": {"locale": "en", "strength": 99},
                    }
                )
            ),
        )
        check_both(
            "unknown field operator rejected",
            command_error_code(
                lambda db: list(db.items.find({"n": {"$bogusOp": 1}}))
            ),
        )
        check_both(
            "unknown top level operator rejected",
            command_error_code(lambda db: list(db.items.find({"$bogus": []}))),
        )
        check_both(
            "unknown operator inside $nor rejected",
            command_error_code(
                lambda db: list(db.items.find({"$nor": [{"n": {"$bogusOp": 1}}]}))
            ),
        )
        check_both(
            "unknown operator in $lookup pipeline rejected",
            command_error_code(
                lambda db: list(
                    db.items.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "text_items",
                                    "let": {"n": "$n"},
                                    "pipeline": [{"$match": {"n": {"$bogusOp": "$$n"}}}],
                                    "as": "joined",
                                }
                            }
                        ]
                    )
                )
            ),
        )
        check_both(
            "unknown operator in $facet pipeline rejected",
            command_error_code(
                lambda db: list(
                    db.items.aggregate(
                        [{"$facet": {"a": [{"$match": {"n": {"$bogusOp": 1}}}]}}]
                    )
                )
            ),
        )
        check_both(
            "unknown command error",
            command_error_code(lambda db: db.command({"definitelyUnknownCommand": 1})),
        )
    finally:
        if os.environ.get("MONGODB_CONFORMANCE_KEEP", "").lower() not in (
            "1",
            "true",
            "yes",
        ):
            for client, name in ((nonodb, nonodb_name), (mongodb, mongodb_name)):
                try:
                    client.drop_database(name)
                except Exception as error:  # noqa: BLE001 - cleanup is best effort
                    print(f"cleanup warning for {name}: {error}", file=sys.stderr)
        nonodb.close()
        mongodb.close()

    if failures:
        print(f"{failures} conformance check(s) failed")
        return 1
    print("mongo conformance test: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
