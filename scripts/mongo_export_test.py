#!/usr/bin/env python3
"""Check MongoDB-compatible export and restore around the NonoDB wire listener.

The daemon must be running with `NONODB_MONGO_ADDR` enabled. The script creates
a temporary database, writes MongoDB BSON values, then:

1. runs `mongoexport` in relaxed and canonical JSON modes;
2. runs `mongodump` and `bsondump`;
3. optionally imports the canonical JSON and restores the dump into a real
   MongoDB using `MONGODB_EXPORT_URL` (or `MONGODB_CONFORMANCE_URL`);
4. compares the restored documents with the NonoDB source.

    NONODB_MONGO_URL='mongodb://127.0.0.1:37017/?directConnection=true' \
    MONGODB_EXPORT_URL='mongodb://127.0.0.1:27017/?directConnection=true' \
    python3 scripts/mongo_export_test.py

Set `MONGODB_EXPORT_KEEP=1` to keep the temporary databases and dump directory.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timezone

from bson import (
    Binary,
    Decimal128,
    MaxKey,
    MinKey,
    ObjectId,
    Timestamp,
    json_util,
)
from pymongo import MongoClient


DOCS = [
    {
        "_id": "a",
        "n": 1,
        "nested": {"v": True},
        "arr": [1, 2, 3],
        "when": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "dec": Decimal128("1.50"),
        "bin": Binary(b"\x00\x01\xff"),
        "ts": Timestamp(1_700_000_000, 7),
        "low": MinKey(),
        "high": MaxKey(),
        "oid": ObjectId("64b64c000000000000000001"),
    },
    {
        "_id": "b",
        "n": 2,
        "nested": {"v": False},
        "nil": None,
        "dec": Decimal128("NaN"),
    },
]


def canonical(value):
    if isinstance(value, dict):
        return {key: canonical(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
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


def display_command(command):
    return " ".join(
        "--uri=mongodb://<redacted>"
        if argument.startswith(("--uri=mongodb://", "--uri=mongodb+srv://"))
        else "mongodb://<redacted>"
        if argument.startswith(("mongodb://", "mongodb+srv://"))
        else argument
        for argument in command
    )


def run(command):
    try:
        return subprocess.run(
            command, check=True, capture_output=True, text=True
        )
    except subprocess.CalledProcessError as error:
        print(f"FAIL command: {display_command(command)}", file=sys.stderr)
        if error.stdout:
            print(error.stdout, file=sys.stderr)
        if error.stderr:
            print(error.stderr, file=sys.stderr)
        raise


def read_json_lines(path):
    docs = []
    with open(path, encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line:
                docs.append(json_util.loads(line))
    return docs


def read_bsondump(path):
    docs = []
    for line in run(["bsondump", "--quiet", path]).stdout.splitlines():
        line = line.strip()
        if line:
            docs.append(json_util.loads(line))
    return docs


def compare(name, expected, actual):
    left = canonical(expected)
    right = canonical(actual)
    if left != right:
        print(f"FAIL {name}")
        print(f"  NonoDB:  {left!r}")
        print(f"  restored: {right!r}")
        return False
    print(f"ok   {name}")
    return True


def main() -> int:
    nonodb_url = os.environ.get(
        "NONODB_MONGO_URL", "mongodb://127.0.0.1:37017/?directConnection=true"
    )
    mongodb_url = os.environ.get("MONGODB_EXPORT_URL") or os.environ.get(
        "MONGODB_CONFORMANCE_URL"
    )
    suffix = uuid.uuid4().hex[:10]
    nonodb_name = f"nonodb_export_{suffix}"
    mongodb_name = f"mongo_export_{suffix}"
    collection = "items"
    keep = os.environ.get("MONGODB_EXPORT_KEEP", "").lower() in (
        "1",
        "true",
        "yes",
    )
    workdir = tempfile.mkdtemp(prefix="nonodb-export-")
    nonodb = MongoClient(nonodb_url, serverSelectionTimeoutMS=5000)
    mongodb = (
        MongoClient(mongodb_url, serverSelectionTimeoutMS=5000)
        if mongodb_url
        else None
    )
    failures = 0
    try:
        nonodb[nonodb_name][collection].insert_many(DOCS)
        nonodb[nonodb_name][collection].create_index("n")
        source = list(nonodb[nonodb_name][collection].find().sort([("_id", 1)]))

        relaxed_path = os.path.join(workdir, "items.json")
        canonical_path = os.path.join(workdir, "items-canonical.json")
        dump_path = os.path.join(workdir, "dump")
        run(
            [
                "mongoexport",
                f"--uri={nonodb_url}",
                f"--db={nonodb_name}",
                f"--collection={collection}",
                f"--out={relaxed_path}",
            ]
        )
        run(
            [
                "mongoexport",
                f"--uri={nonodb_url}",
                f"--db={nonodb_name}",
                f"--collection={collection}",
                "--jsonFormat=canonical",
                f"--out={canonical_path}",
            ]
        )
        run(
            [
                "mongodump",
                f"--uri={nonodb_url}",
                f"--db={nonodb_name}",
                f"--out={dump_path}",
            ]
        )
        relaxed = read_json_lines(relaxed_path)
        canonical_docs = read_json_lines(canonical_path)
        bson_path = os.path.join(dump_path, nonodb_name, f"{collection}.bson")
        dumped = read_bsondump(bson_path)
        if not compare("mongoexport relaxed", source, relaxed):
            failures += 1
        if not compare("mongoexport canonical", source, canonical_docs):
            failures += 1
        if not compare("mongodump bson", source, dumped):
            failures += 1

        if mongodb is None:
            print("restore checks skipped; set MONGODB_EXPORT_URL to enable them")
        else:
            mongodb.drop_database(mongodb_name)
            run(
                [
                    "mongoimport",
                    f"--uri={mongodb_url}",
                    f"--db={mongodb_name}",
                    f"--collection=imported_{collection}",
                    f"--file={canonical_path}",
                ]
            )
            imported = list(
                mongodb[mongodb_name][f"imported_{collection}"]
                .find()
                .sort([("_id", 1)])
            )
            if not compare("mongoimport canonical round-trip", source, imported):
                failures += 1

            run(
                [
                    "mongorestore",
                    f"--uri={mongodb_url}",
                    "--drop",
                    f"--nsFrom={nonodb_name}.*",
                    f"--nsTo={mongodb_name}.*",
                    dump_path,
                ]
            )
            restored = list(
                mongodb[mongodb_name][collection].find().sort([("_id", 1)])
            )
            if not compare("mongorestore bson round-trip", source, restored):
                failures += 1

            expected_indexes = sorted(
                tuple(index["key"].items())
                for index in nonodb[nonodb_name][collection].list_indexes()
            )
            actual_indexes = sorted(
                tuple(index["key"].items())
                for index in mongodb[mongodb_name][collection].list_indexes()
            )
            if compare(
                "restored index keys", expected_indexes, actual_indexes
            ):
                pass
            else:
                failures += 1
    finally:
        if not keep:
            for client, name in ((nonodb, nonodb_name), (mongodb, mongodb_name)):
                if client is not None:
                    try:
                        client.drop_database(name)
                    except Exception as error:  # noqa: BLE001 - cleanup is best effort
                        print(
                            f"cleanup warning for {name}: {error}", file=sys.stderr
                        )
            shutil.rmtree(workdir, ignore_errors=True)
        else:
            print(f"kept export directory: {workdir}")
        nonodb.close()
        if mongodb is not None:
            mongodb.close()

    if failures:
        print(f"{failures} export check(s) failed")
        return 1
    print("mongo export portability test: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
