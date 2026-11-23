#!/usr/bin/env python3
"""Copy a MongoDB database into NonoDB.

This is a migration helper, not a live replication feed. Stop writes to the
source (or use a consistent snapshot) before running it. It discovers
collections and indexes, converts BSON values to Extended JSON, inserts in
batches, translates common index DDL, and verifies document counts and a
content checksum.

Example:

    python3 scripts/nonodb-migrate.py \
      --mongo-uri 'mongodb://127.0.0.1:27017/app' \
      --source-db app \
      --socket /run/nonodb/nonodb.sock \
      --database app \
      --report /tmp/app-migration.json

The source URI may contain credentials. They are never written to the report.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit, urlunsplit

from bson import ObjectId, json_util
from pymongo import MongoClient
from pymongo.errors import PyMongoError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402


class MigrationFailure(Exception):
    """A source, target, or validation error."""


@dataclass
class IndexPlan:
    fields: list[str]
    unique: bool = False
    sparse: bool = False
    partial_filter_expression: dict | None = None
    geo: bool = False
    text: bool = False


@dataclass
class CollectionPlan:
    name: str
    ttl_seconds: int | None = None
    ttl_field: str | None = None
    indexes: list[IndexPlan] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    count: int = 0
    bytes: int = 0
    checksum: str = ""


def fail(message: str) -> None:
    raise MigrationFailure(message)


def redact_uri(uri: str) -> str:
    """Remove userinfo from a MongoDB URI before putting it in a report."""
    parts = urlsplit(uri)
    if not parts.username and not parts.password:
        return uri
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment))


def extended(doc: dict[str, Any]) -> dict[str, Any]:
    """Convert PyMongo BSON values to the Extended JSON shape NonoDB accepts."""
    encoded = json_util.dumps(doc, json_options=json_util.RELAXED_JSON_OPTIONS)
    value = json.loads(encoded)
    if not isinstance(value, dict):
        fail("document conversion did not produce an object")
    return value


def normalized(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: normalized(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalized(item) for item in value]
    return value


def document_bytes(doc: dict[str, Any]) -> bytes:
    # NonoDB may return canonical Extended JSON ($date/$numberLong), while a
    # source PyMongo document is encoded as relaxed Extended JSON ($date ISO).
    # Round-trip through bson.json_util so both sides use one representation.
    decoded = json_util.loads(json.dumps(doc))
    canonical = json.loads(
        json_util.dumps(decoded, json_options=json_util.RELAXED_JSON_OPTIONS)
    )
    return json.dumps(
        normalized(canonical), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def validate_id(doc: dict[str, Any], where: str) -> None:
    if "_id" not in doc:
        fail(f"{where}: document has no _id")
    value = doc["_id"]
    if isinstance(value, str) or isinstance(value, ObjectId):
        return
    fail(
        f"{where}: unsupported _id type {type(value).__name__}; "
        "NonoDB supports string and ObjectId _id values"
    )


def discover(db: Any, include: set[str], exclude: set[str]) -> list[CollectionPlan]:
    plans: list[CollectionPlan] = []
    for info in db.list_collections():
        name = info.get("name")
        if not isinstance(name, str) or name.startswith("system."):
            continue
        if info.get("type") == "view":
            continue
        if include and name not in include:
            continue
        if name in exclude:
            continue

        plan = CollectionPlan(name=name)
        collection = db[name]
        ttl_values: list[int] = []
        for index_name, index in collection.index_information().items():
            if index_name == "_id_":
                continue
            key = index.get("key", [])
            if not key:
                plan.warnings.append(f"index {index_name!r} has no key; skipped")
                continue
            fields = [str(field) for field, _ in key]
            directions = [direction for _, direction in key]
            if any(direction == "text" for direction in directions):
                if len(fields) != 1:
                    plan.warnings.append(
                        f"text index {index_name!r} has several fields; skipped"
                    )
                else:
                    plan.indexes.append(IndexPlan(fields=fields, text=True))
                continue
            if any(direction == "2dsphere" for direction in directions):
                if len(fields) != 1:
                    plan.warnings.append(
                        f"geo index {index_name!r} is compound; skipped"
                    )
                else:
                    plan.indexes.append(IndexPlan(fields=fields, geo=True))
                continue
            if index.get("wildcardProjection"):
                plan.warnings.append(
                    f"index {index_name!r} uses wildcardProjection; skipped"
                )
                continue
            partial_filter = index.get("partialFilterExpression")
            if partial_filter is not None and not isinstance(partial_filter, dict):
                plan.warnings.append(
                    f"index {index_name!r} has a non-document partialFilterExpression; skipped"
                )
                continue
            if any(not isinstance(direction, int) for direction in directions):
                plan.warnings.append(
                    f"index {index_name!r} has unsupported key types; skipped"
                )
                continue
            plan.indexes.append(
                IndexPlan(
                    fields=fields,
                    unique=bool(index.get("unique", False)),
                    sparse=bool(index.get("sparse", False)),
                    partial_filter_expression=partial_filter,
                )
            )
            if "expireAfterSeconds" in index:
                seconds = index["expireAfterSeconds"]
                if (
                    isinstance(seconds, bool)
                    or not isinstance(seconds, (int, float))
                    or not float(seconds).is_integer()
                    or seconds <= 0
                ):
                    fail(f"collection {name!r}: unsupported expireAfterSeconds={seconds!r}")
                ttl_values.append(int(seconds))
        if len(set(ttl_values)) > 1:
            fail(f"collection {name!r}: multiple TTL indexes are not supported")
        if ttl_values:
            plan.ttl_seconds = ttl_values[0]
            plan.ttl_field = next(
                (
                    fields[0]
                    for index_name, index in collection.index_information().items()
                    if index_name != "_id_"
                    and "expireAfterSeconds" in index
                    and len(index.get("key", [])) == 1
                ),
                None,
            )
            if plan.ttl_field is None:
                fail(f"collection {name!r}: cannot determine the TTL date field")
        plans.append(plan)
    return plans


def iter_documents(collection: Any, batch_size: int) -> Iterable[tuple[dict[str, Any], bytes]]:
    cursor = collection.find({}).sort([("_id", 1)]).batch_size(batch_size)
    for doc in cursor:
        validate_id(doc, f"collection {collection.name!r}")
        converted = extended(doc)
        yield converted, document_bytes(converted)


def copy_collection(
    source: Any,
    target_db: NonoDB,
    plan: CollectionPlan,
    *,
    batch_size: int,
    max_bytes: int,
    drop: bool,
) -> None:
    target_names = set(target_db.collections())
    if plan.name in target_names:
        if drop:
            target_db.exec({"op": "drop_collection", "coll": plan.name})
        else:
            existing = target_db.collection(plan.name).count()
            if existing:
                fail(
                    f"target collection {plan.name!r} already has {existing} documents; "
                    "use --drop or an empty target"
                )

    if plan.ttl_seconds is not None and plan.ttl_field is None:
        if plan.name in target_db.collections():
            target_db.set_collection_ttl(plan.name, plan.ttl_seconds)
        else:
            target_db.create_collection(plan.name, ttl_seconds=plan.ttl_seconds)

    target = target_db.collection(plan.name)
    source_collection = source[plan.name]
    digest = hashlib.sha256()
    batch: list[dict[str, Any]] = []
    batch_bytes = 0
    count = 0
    total_bytes = 0
    first_id: Any = None
    pretty_first_id = ""

    def flush() -> None:
        nonlocal batch, batch_bytes
        if not batch:
            return
        try:
            target.insert_many(batch)
        except NonoDBError as exc:
            raise MigrationFailure(
                f"collection {plan.name!r}, first _id {pretty_first_id!r}: "
                f"insert_many failed: {exc.code}: {exc.message}"
            ) from exc
        batch = []
        batch_bytes = 0

    for doc, data in iter_documents(source_collection, batch_size):
        size = len(data)
        if first_id is None:
            first_id = doc.get("_id")
            pretty_first_id = json.dumps(normalized(first_id), sort_keys=True)
        if batch and (len(batch) >= batch_size or batch_bytes + size > max_bytes):
            flush()
        batch.append(doc)
        batch_bytes += size
        count += 1
        total_bytes += size
        digest.update(size.to_bytes(8, "big"))
        digest.update(data)

    flush()

    source_count = source_collection.count_documents({})
    if source_count != count:
        fail(
            f"collection {plan.name!r}: source changed during migration "
            f"({source_count} counted, {count} copied)"
        )
    target_count = target.count()
    if target_count != count:
        fail(
            f"collection {plan.name!r}: target count {target_count} != copied {count}"
        )

    target_digest = hashlib.sha256()
    for doc in target.find({}, sort=[("_id", 1)]):
        data = document_bytes(doc)
        target_digest.update(len(data).to_bytes(8, "big"))
        target_digest.update(data)
    if target_digest.digest() != digest.digest():
        fail(f"collection {plan.name!r}: target checksum differs from source")

    plan.count = count
    plan.bytes = total_bytes
    plan.checksum = digest.hexdigest()

    for index in plan.indexes:
        if index.text:
            target.create_text_index(index.fields[0])
        elif index.geo:
            target.create_index(field=index.fields[0], geo=True)
        else:
            target.create_indexes(
                [
                    {
                        "fields": index.fields,
                        "unique": index.unique,
                        "sparse": index.sparse,
                        "partial_filter_expression": index.partial_filter_expression,
                    }
                ]
            )

    if plan.ttl_seconds is not None and plan.ttl_field is not None:
        target_db.set_collection_ttl_after(
            plan.name, plan.ttl_field, plan.ttl_seconds
        )
        target_db.backfill_collection_ttl_all(
            plan.name, plan.ttl_field, plan.ttl_seconds
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Copy a MongoDB database into NonoDB with verification."
    )
    parser.add_argument("--mongo-uri", required=True, help="MongoDB source URI")
    parser.add_argument("--source-db", required=True, help="source database name")
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--socket", help="NonoDB Unix socket path")
    target.add_argument("--url", help="NonoDB HTTP/TLS base URL")
    parser.add_argument("--token", help="NonoDB bearer token; defaults to NONODB_TOKEN")
    parser.add_argument("--database", default="default", help="target NonoDB database")
    parser.add_argument(
        "--no-create-database",
        action="store_true",
        help="do not create the target database when it is missing",
    )
    parser.add_argument(
        "--include", action="append", default=[], help="only these collections (repeatable)"
    )
    parser.add_argument(
        "--exclude", action="append", default=[], help="skip these collections (repeatable)"
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-bytes", type=int, default=3_000_000)
    parser.add_argument(
        "--drop", action="store_true", help="drop non-empty target collections first"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="discover and validate without writing"
    )
    parser.add_argument("--report", type=Path, help="write a JSON migration report")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.batch_size <= 0:
        print("--batch-size must be positive", file=sys.stderr)
        return 2
    if args.max_bytes <= 0:
        print("--max-bytes must be positive", file=sys.stderr)
        return 2

    include = set(args.include)
    exclude = set(args.exclude)
    started = datetime.now(timezone.utc).isoformat()
    source_client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5_000)
    source = source_client[args.source_db]
    target_db: NonoDB | None = None
    plans: list[CollectionPlan] = []
    report: dict[str, Any] = {
        "version": 1,
        "started_at": started,
        "source": {
            "database": args.source_db,
            "uri": redact_uri(args.mongo_uri),
        },
        "target": {"database": args.database, "dry_run": args.dry_run},
        "collections": [],
        "ok": False,
    }
    try:
        source_client.admin.command("ping")
        plans = discover(source, include, exclude)
        if not plans:
            fail("no matching collections found in the source database")
        if not args.dry_run:
            if args.database != "default" and not args.no_create_database:
                admin = NonoDB(
                    socket_path=args.socket,
                    base_url=args.url,
                    token=args.token,
                )
                try:
                    admin.exec({"op": "create_database", "name": args.database})
                except NonoDBError as exc:
                    if exc.code != "invalid_query" or "already exists" not in exc.message:
                        raise
                finally:
                    admin.close()
            target_db = NonoDB(
                socket_path=args.socket,
                base_url=args.url,
                token=args.token,
                database=args.database,
            )
            if not target_db.health():
                fail("NonoDB health check failed")

        for plan in plans:
            print(f"Collection {plan.name!r}", flush=True)
            if plan.ttl_seconds is not None:
                suffix = (
                    f" from {plan.ttl_field}" if plan.ttl_field is not None else ""
                )
                print(f"  TTL: {plan.ttl_seconds}s{suffix}", flush=True)
            for warning in plan.warnings:
                print(f"  warning: {warning}", flush=True)
            if args.dry_run:
                plan.count = source[plan.name].count_documents({})
                for doc in source[plan.name].find({}).sort([("_id", 1)]):
                    validate_id(doc, f"collection {plan.name!r}")
                print(f"  dry-run: {plan.count} documents", flush=True)
            else:
                assert target_db is not None
                copy_collection(
                    source,
                    target_db,
                    plan,
                    batch_size=args.batch_size,
                    max_bytes=args.max_bytes,
                    drop=args.drop,
                )
                print(
                    f"  copied {plan.count} documents, {plan.bytes} bytes, "
                    f"sha256={plan.checksum}",
                    flush=True,
                )
            report["collections"].append(
                {
                    "name": plan.name,
                    "count": plan.count,
                    "bytes": plan.bytes,
                    "sha256": plan.checksum,
                    "ttl_seconds": plan.ttl_seconds,
                    "ttl_field": plan.ttl_field,
                    "indexes": [
                        {
                            "fields": index.fields,
                            "unique": index.unique,
                            "sparse": index.sparse,
                            "partial_filter_expression": index.partial_filter_expression,
                            "geo": index.geo,
                            "text": index.text,
                        }
                        for index in plan.indexes
                    ],
                    "warnings": plan.warnings,
                }
            )
        report["ok"] = True
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(
            f"{'Validated' if args.dry_run else 'Migrated'} "
            f"{sum(plan.count for plan in plans)} documents in {len(plans)} collections."
        )
        return 0
    except (MigrationFailure, NonoDBError, PyMongoError) as exc:
        report["error"] = str(exc)
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(f"migration failed: {exc}", file=sys.stderr)
        return 1
    finally:
        source_client.close()
        if target_db is not None:
            target_db.close()


if __name__ == "__main__":
    raise SystemExit(main())
