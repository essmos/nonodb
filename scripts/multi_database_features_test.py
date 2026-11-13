#!/usr/bin/env python3
"""Feature matrix inside a non-default database.

Covers CRUD, complex filters, lookup, aggregation, BM25, fuzzy, vector search,
TTL, history/versioning, automatic events, subscriptions, auto-index, and jobs.
"""

from __future__ import annotations

import base64
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402

DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))


def wait_for_socket(path: Path, proc: subprocess.Popen) -> None:
    for _ in range(200):
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited with {proc.returncode}")
        time.sleep(0.05)
    raise RuntimeError("daemon socket did not appear")


def wait_for(fn, timeout: float = 15.0, message: str = "condition not met") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fn():
            return
        time.sleep(0.1)
    raise AssertionError(message)


def main() -> int:
    if not DAEMON.is_file():
        raise SystemExit(f"missing daemon binary: {DAEMON}")
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-multi-features-"))
    data = tmp / "data"
    sock = tmp / "nonodb.sock"
    env = os.environ.copy()
    env.update(
        NONODB_DATA=str(data),
        NONODB_SOCK=str(sock),
        NONODB_LOG="warn",
        NONODB_TTL_SWEEP_INTERVAL="1",
        NONODB_MAINTENANCE_INTERVAL_MS="100",
        NONODB_MAX_OPEN_DATABASES="4",
        NONODB_CACHE_TTL="0",
        NONODB_AUTO_INDEX="1",
        NONODB_AUTO_INDEX_MIN_HITS="2",
        NONODB_AUTO_INDEX_MIN_DOCS="1",
        NONODB_AUTO_INDEX_INTERVAL="1",
        NONODB_AUTO_INDEX_WINDOW="60",
    )
    proc = subprocess.Popen(
        [str(DAEMON)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    try:
        wait_for_socket(sock, proc)
        root = NonoDB(str(sock), cache_ttl=0.0)
        root.exec({"op": "create_database", "name": "app1"})
        root.close()
        db = NonoDB(str(sock), cache_ttl=0.0, database="app1")

        items = db.collection("feat_items")
        items.delete({}, multi=True)
        items.insert_many([
            {
                "_id": "i1",
                "kind": "a",
                "sku": "A",
                "price": 10,
                "tags": ["rust", "db"],
                "title": "Rust Database",
                "body": "rust search engine database",
                "embedding": [1.0, 0.0],
                "auto_field": "auto-a",
            },
            {
                "_id": "i2",
                "kind": "b",
                "sku": "B",
                "price": 20,
                "tags": ["python"],
                "title": "Python Service",
                "body": "python web service",
                "embedding": [0.0, 1.0],
                "auto_field": "auto-b",
            },
            {
                "_id": "i3",
                "kind": "a",
                "sku": "C",
                "price": 30,
                "tags": ["rust"],
                "title": "Rust Engine",
                "body": "fast rust search",
                "embedding": [0.7, 0.3],
                "auto_field": "auto-a",
            },
        ])

        rows = items.find(
            {"$and": [{"price": {"$gte": 10}}, {"$or": [{"kind": "a"}, {"tags": "python"}]}]},
            sort=[("price", 1)],
        )
        assert [row["_id"] for row in rows] == ["i1", "i2", "i3"], rows
        assert [row["_id"] for row in items.find({"title": {"$regex": "^Rust"}})] == ["i1", "i3"]
        assert items.count({"tags": {"$all": ["rust"]}}) == 2

        items.create_index("sku", unique=True)
        assert items.lookup("sku", ["A", "C"])[0]["_id"] == "i1"
        assert items.lookup("sku", ["A", "C"])[1]["_id"] == "i3"

        groups = items.aggregate([
            {"$match": {"price": {"$gte": 5}}},
            {"$group": {"_id": "$kind", "total": {"$sum": "$price"}, "n": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ])
        assert groups == [
            {"_id": "a", "total": 40, "n": 2},
            {"_id": "b", "total": 20, "n": 1},
        ], groups

        items.create_text_index("body")
        assert items.search("body", "rust")[0]["_id"] in {"i1", "i3"}
        items.create_fuzzy_index("title")
        fuzzy = items.fuzzy("title", "Databse", max_distance=2)
        assert fuzzy and fuzzy[0]["_id"] == "i1", fuzzy
        items.create_vector_index("embedding", 2, "cosine")
        vectors = items.vector_search("embedding", [1.0, 0.0], limit=3)
        assert vectors[0]["_id"] == "i1", vectors

        # Auto-index is per database and persisted in data/app1/auto-index.json.
        for _ in range(4):
            items.find({"auto_field": "auto-a"})
        wait_for(
            lambda: any(
                index.get("fields") == ["auto_field"] for index in items.list_indexes()
            ),
            message="automatic index was not created in app1",
        )
        wait_for(
            lambda: (data / "app1" / "auto-index.json").is_file(),
            message="app1 auto-index state file missing",
        )

        # TTL in app1.
        db.create_collection("feat_ttl", ttl_seconds=1)
        ttl = db.collection("feat_ttl")
        ttl.insert({"_id": "ttl1", "v": 1})
        wait_for(lambda: ttl.find_one({"_id": "ttl1"}) is None, message="TTL did not expire")

        # History and exact versions in app1.
        db.create_collection("feat_history")
        db.set_collection_history("feat_history", 10)
        history = db.collection("feat_history")
        history.insert({"_id": "h1", "v": 1})
        history.update({"_id": "h1"}, {"$set": {"v": 2}})
        history.update({"_id": "h1"}, {"$set": {"v": 3}})
        revisions = history.history("h1", include_docs=True)
        assert len(revisions) >= 2, revisions
        version = revisions[0]["rev"]
        assert history.get_version("h1", version) is not None

        # Vault encryption, tenant isolation, and wrong-key failure in app1.
        key = base64.b64encode(os.urandom(32)).decode()
        other_key = base64.b64encode(os.urandom(32)).decode()
        db.create_collection("feat_vault", "vault")
        vault = db.vault("acme", "acme:v1", key).collection("feat_vault")
        vault_inserted = vault.insert(
            {"tenant_id": "acme", "secret": "s3cret", "payload": {"ssn": "123-45-6789"}}
        )
        assert vault.find_one({"_id": vault_inserted})["secret"] == "s3cret"
        try:
            db.vault("acme", "acme:v1", other_key).collection("feat_vault").find_one(
                {"_id": vault_inserted}
            )
        except Exception as exc:
            assert "decryption_failed" in str(exc), exc
        else:
            raise AssertionError("vault accepted the wrong key in app1")
        assert (
            db.vault("other", "acme:v1", key)
            .collection("feat_vault")
            .find_one({"_id": vault_inserted})
            is None
        )
        assert vault.delete({"_id": vault_inserted}) == 1

        # Automatic events and conditional subscriptions in app1.
        db.create_collection("feat_events")
        db.set_collection_events("feat_events", True, [])
        db.create_subscription("feat_events", "paid", filter={"status": "paid"})
        events = db.collection("feat_events")
        events.insert({"_id": "e1", "status": "new"})
        events.insert({"_id": "e2", "status": "paid"})
        outbox = db.collection("_events")
        wait_for(
            lambda: outbox.count({"type": "feat_events.insert"}) == 2,
            message="automatic events missing in app1",
        )
        wait_for(
            lambda: outbox.count({"type": "subscription.paid"}) == 1,
            message="conditional subscription missing in app1",
        )
        assert db.list_subscriptions("feat_events")[0]["name"] == "paid"

        # Background jobs/scheduler helper against app1 via NONODB_DB.
        jobs_env = env.copy()
        jobs_env.update(
            NONODB_SOCK=str(sock),
            NONODB_DB="app1",
            NONODB_JOBS_COLLECTION="feat_jobs",
            PYTHONPATH=str(ROOT / "client") + os.pathsep + str(ROOT / "workers/python"),
        )
        subprocess.run(
            [sys.executable, str(ROOT / "workers/python/examples/jobs_smoke.py")],
            check=True,
            env=jobs_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=60,
        )

        # Unknown database must not be created by a client typo.
        missing = NonoDB(str(sock), database="typo")
        try:
            missing.exec({"op": "list_collections"})
        except Exception as exc:
            assert "not_found" in str(exc), exc
        else:
            raise AssertionError("unknown database was created implicitly")
        finally:
            missing.close()

        db.close()
        print("multi-database feature matrix: ok")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
