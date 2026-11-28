#!/usr/bin/env python3
"""Unit test for columnar target creation in scripts/nonodb-migrate.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "nonodb_migrate", ROOT / "scripts" / "nonodb-migrate.py"
)
migrate = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = migrate
spec.loader.exec_module(migrate)


class Cursor(list):
    def sort(self, _spec):
        return self

    def batch_size(self, _size):
        return self


class SourceCollection:
    name = "docs"

    def __init__(self, docs):
        self.docs = docs

    def find(self, _filter):
        return Cursor(self.docs)

    def count_documents(self, _filter):
        return len(self.docs)


class Source(dict):
    def __init__(self, docs):
        super().__init__({"docs": SourceCollection(docs)})


class TargetCollection:
    def __init__(self, db):
        self.db = db

    def insert_many(self, docs):
        self.db.docs.extend(docs)
        return [doc.get("_id") for doc in docs]

    def count(self):
        return len(self.db.docs)

    def find(self, _filter, sort=None):
        return sorted(self.db.docs, key=lambda doc: doc.get("_id", ""))


class TargetDB:
    def __init__(self):
        self.created = []
        self.docs = []
        self.ttl = None

    def collections(self):
        return []

    def create_collection(self, name, **kwargs):
        self.created.append((name, kwargs))

    def set_collection_ttl(self, name, ttl):
        self.ttl = (name, ttl)

    def collection(self, _name):
        return TargetCollection(self)


def main() -> int:
    docs = [{"_id": "a", "v": 1}, {"_id": "b", "v": 2}]
    target = TargetDB()
    plan = migrate.CollectionPlan(name="docs")
    migrate.copy_collection(
        Source(docs),
        target,
        plan,
        batch_size=10,
        max_bytes=1_000_000,
        drop=False,
        profile="columnar",
        columnar={"bucket_max_docs": 64, "compression": "max"},
    )
    assert target.created == [
        (
            "docs",
            {
                "profile": "columnar",
                "ttl_seconds": None,
                "columnar": {"bucket_max_docs": 64, "compression": "max"},
            },
        )
    ], target.created
    assert target.docs == docs
    assert plan.count == 2

    target = TargetDB()
    plan = migrate.CollectionPlan(name="docs", ttl_seconds=60)
    migrate.copy_collection(
        Source(docs),
        target,
        plan,
        batch_size=10,
        max_bytes=1_000_000,
        drop=False,
        profile="columnar",
        columnar={"compression": "balanced"},
    )
    assert target.created[0][1]["ttl_seconds"] == 60
    print("columnar migration unit test OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
