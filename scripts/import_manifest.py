#!/usr/bin/env python3
"""Import one or more NDJSON files into nonodb using a manifest.

The manifest format is documented in docs/migration.md and
docs/import-manifest.schema.json.

Each input file contains one JSON object per line. Values may use MongoDB
Extended JSON ($date, $numberLong, $numberDecimal, $binary, $oid, and so on).
The target collection is created on first write unless ttl_seconds is set.

This is a migration helper, not part of the daemon. It uses the thin Python
client and the normal HTTP/JSON write path.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402


class ImportFailure(Exception):
    """A manifest, input file, or import operation failed."""


def _fail(message: str) -> None:
    raise ImportFailure(message)


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
        manifest = json.loads(raw)
    except OSError as exc:
        _fail(f"cannot read manifest {path}: {exc}")
    except ValueError as exc:
        _fail(f"invalid JSON in manifest {path}: {exc}")

    if not isinstance(manifest, dict):
        _fail("manifest must be a JSON object")
    if manifest.get("version") != 1:
        _fail("manifest version must be 1")

    collections = manifest.get("collections")
    if not isinstance(collections, list) or not collections:
        _fail("manifest collections must be a non-empty array")

    names: set[str] = set()
    for index, entry in enumerate(collections):
        where = f"collections[{index}]"
        if not isinstance(entry, dict):
            _fail(f"{where} must be an object")
        name = entry.get("name")
        filename = entry.get("file")
        if not isinstance(name, str) or not name:
            _fail(f"{where}.name must be a non-empty string")
        if name in names:
            _fail(f"duplicate collection name: {name}")
        names.add(name)
        if not isinstance(filename, str) or not filename:
            _fail(f"{where}.file must be a non-empty string")
        file_path = Path(filename)
        if not file_path.is_absolute():
            file_path = path.parent / file_path
        entry["_path"] = file_path.resolve()

        ttl = entry.get("ttl_seconds")
        if ttl is not None and (not isinstance(ttl, int) or isinstance(ttl, bool) or ttl <= 0):
            _fail(f"{where}.ttl_seconds must be a positive integer or null")

        indexes = entry.get("indexes", [])
        if not isinstance(indexes, list):
            _fail(f"{where}.indexes must be an array")
        for idx, definition in enumerate(indexes):
            iwhere = f"{where}.indexes[{idx}]"
            if not isinstance(definition, dict):
                _fail(f"{iwhere} must be an object")
            fields = definition.get("fields")
            if not isinstance(fields, list) or not fields or not all(
                isinstance(field, str) and field for field in fields
            ):
                _fail(f"{iwhere}.fields must be a non-empty string array")
            unique = definition.get("unique", False)
            if not isinstance(unique, bool):
                _fail(f"{iwhere}.unique must be a boolean")
            include = definition.get("include", [])
            if not isinstance(include, list) or not all(
                isinstance(field, str) and field for field in include
            ):
                _fail(f"{iwhere}.include must be a string array")

    return manifest


def _stringify_id(value: Any, where: str) -> str:
    """Map common source _id types to a stable string.

    nonodb requires _id to be a string. ObjectId and UUID values are converted
    to their canonical text form; integer ids become decimal strings.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, bool) or value is None:
        _fail(f"{where}: _id must be a string, ObjectId, UUID, binary, or number")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return repr(value)
    if isinstance(value, dict):
        if set(value) == {"$oid"} and isinstance(value["$oid"], str):
            oid = value["$oid"].lower()
            if len(oid) == 24 and all(char in "0123456789abcdef" for char in oid):
                return oid
            _fail(f"{where}: invalid $oid in _id")
        if set(value) == {"$uuid"} and isinstance(value["$uuid"], str):
            return value["$uuid"].lower()
        if "$binary" in value and isinstance(value["$binary"], dict):
            payload = value["$binary"]
            if isinstance(payload.get("base64"), str):
                return payload["base64"]
    _fail(f"{where}: unsupported _id type; convert it to a string before import")


def _iter_documents(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    try:
        handle = path.open("r", encoding="utf-8")
    except OSError as exc:
        _fail(f"cannot read input file {path}: {exc}")

    with handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            where = f"{path}:{line_number}"
            try:
                doc = json.loads(line)
            except ValueError as exc:
                _fail(f"{where}: invalid JSON: {exc}")
            if not isinstance(doc, dict):
                _fail(f"{where}: each line must contain a JSON object")
            if "_id" in doc:
                doc["_id"] = _stringify_id(doc["_id"], where)
            yield line_number, doc


def _encoded_size(doc: dict[str, Any]) -> int:
    return len(json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def _import_collection(
    db: NonoDB,
    entry: dict[str, Any],
    *,
    batch_size: int,
    max_bytes: int,
    dry_run: bool,
) -> tuple[int, int]:
    name = entry["name"]
    path = entry["_path"]
    if not path.is_file():
        _fail(f"input file for collection {name!r} does not exist: {path}")

    if not dry_run and entry.get("ttl_seconds") is not None:
        db.create_collection(name, ttl_seconds=entry["ttl_seconds"])

    batch: list[dict[str, Any]] = []
    batch_bytes = 0
    first_line = 0
    last_line = 0
    count = 0
    total_bytes = 0
    collection = db.collection(name) if db is not None else None

    def flush() -> None:
        nonlocal batch, batch_bytes, first_line, last_line
        if not batch:
            return
        if dry_run:
            batch = []
            batch_bytes = 0
            first_line = 0
            last_line = 0
            return
        try:
            collection.insert_many(batch)
        except NonoDBError as exc:
            _fail(
                f"collection {name!r}, lines {first_line}-{last_line}: "
                f"{exc.code}: {exc.message}"
            )
        print(
            f"  {name}: imported lines {first_line}-{last_line} "
            f"({len(batch)} documents)",
            flush=True,
        )
        batch = []
        batch_bytes = 0
        first_line = 0
        last_line = 0

    for line_number, doc in _iter_documents(path):
        size = _encoded_size(doc)
        if size > max_bytes:
            _fail(
                f"{path}:{line_number}: document is {size} bytes, "
                f"above --max-bytes={max_bytes}"
            )
        if batch and (len(batch) >= batch_size or batch_bytes + size > max_bytes):
            flush()
        if not batch:
            first_line = line_number
        batch.append(doc)
        batch_bytes += size
        last_line = line_number
        count += 1
        total_bytes += size

    if not dry_run:
        flush()

    if not dry_run:
        for definition in entry.get("indexes", []):
            try:
                collection.create_index(
                    fields=definition["fields"],
                    unique=definition.get("unique", False),
                    include=definition.get("include") or None,
                )
            except NonoDBError as exc:
                _fail(
                    f"collection {name!r}: could not create index "
                    f"{definition['fields']}: {exc.code}: {exc.message}"
                )

    return count, total_bytes


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import NDJSON files into nonodb using a v1 import manifest."
    )
    parser.add_argument("manifest", type=Path, help="path to import-manifest.json")
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--socket", help="Unix socket path (default: NONODB_SOCK)")
    target.add_argument("--url", help="HTTP/TLS base URL, e.g. https://db:7878")
    parser.add_argument(
        "--token",
        help="bearer token; defaults to NONODB_TOKEN",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="maximum documents per insert_many request (default: 500)",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=3_000_000,
        help="maximum encoded bytes per request (default: 3000000)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate the manifest and files without writing",
    )
    parser.add_argument(
        "--skip-indexes",
        action="store_true",
        help="import documents but do not create manifest indexes",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.batch_size <= 0:
        print("--batch-size must be positive", file=sys.stderr)
        return 2
    if args.max_bytes <= 0:
        print("--max-bytes must be positive", file=sys.stderr)
        return 2

    try:
        manifest = _load_manifest(args.manifest.resolve())
        if args.skip_indexes:
            for entry in manifest["collections"]:
                entry["indexes"] = []

        db = None
        if not args.dry_run:
            db = NonoDB(
                socket_path=args.socket,
                base_url=args.url,
                token=args.token,
            )
            if not db.health():
                _fail("nonodb health check failed")

        total_docs = 0
        total_bytes = 0
        for entry in manifest["collections"]:
            print(
                f"Collection {entry['name']!r} <- {entry['_path']}",
                flush=True,
            )
            count, size = _import_collection(
                db,
                entry,
                batch_size=args.batch_size,
                max_bytes=args.max_bytes,
                dry_run=args.dry_run,
            )
            total_docs += count
            total_bytes += size
            action = "validated" if args.dry_run else "imported"
            print(f"  {action} {count} documents ({size} bytes)", flush=True)

        if db is not None:
            db.close()

        print(
            f"{'Would import' if args.dry_run else 'Imported'} "
            f"{total_docs} documents from {len(manifest['collections'])} collections "
            f"({total_bytes} bytes)."
        )
        return 0
    except ImportFailure as exc:
        print(f"import failed: {exc}", file=sys.stderr)
        return 1
    except NonoDBError as exc:
        print(f"nonodb error: {exc.code}: {exc.message}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
