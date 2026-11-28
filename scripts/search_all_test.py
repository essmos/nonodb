#!/usr/bin/env python3
"""Integration test for search_all and Atlas-style search aggregation stages."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def daemon_binary() -> Path:
    configured = os.environ.get("NONODB_DAEMON")
    path = Path(configured) if configured else ROOT / "target" / "debug" / "nonodb-daemon"
    if not path.is_file():
        path = ROOT / "target" / "release" / "nonodb-daemon"
    if not path.is_file():
        raise SystemExit(f"missing daemon binary: {path}")
    return path


def wait_socket(path: Path, proc: subprocess.Popen, timeout: float = 20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited with {proc.returncode}")
        if path.exists():
            return
        time.sleep(0.05)
    raise RuntimeError("daemon socket did not appear")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="nonodb-search-") as tmp_name:
        tmp = Path(tmp_name)
        sock = tmp / "nonodb.sock"
        mongo_port = free_port()
        env = os.environ.copy()
        env.update(
            {
                "NONODB_DATA": str(tmp / "data"),
                "NONODB_SOCK": str(sock),
                "NONODB_MAP_SIZE": "64MiB",
                "NONODB_MONGO_ADDR": f"127.0.0.1:{mongo_port}",
                "RUST_LOG": "warn",
            }
        )
        log = (tmp / "daemon.log").open("wb")
        proc = subprocess.Popen(
            [str(daemon_binary())], env=env, stdout=log, stderr=subprocess.STDOUT
        )
        try:
            wait_socket(sock, proc)
            db = NonoDB(str(sock), timeout=30)
            products = db.collection("products")
            products.insert_many(
                [
                    {
                        "_id": "a",
                        "title": "coffee beans",
                        "description": "fresh roasted beans",
                        "status": "active",
                        "price": 20,
                        "embedding": [1.0, 0.0],
                    },
                    {
                        "_id": "b",
                        "title": "coffee grinder",
                        "description": "burr grinder",
                        "status": "active",
                        "price": 30,
                        "embedding": [0.9, 0.1],
                    },
                    {
                        "_id": "c",
                        "title": "tea",
                        "description": "green tea",
                        "status": "hidden",
                        "price": 5,
                        "embedding": [0.0, 1.0],
                    },
                ]
            )
            products.create_text_index("title")
            products.create_text_index("description")
            products.create_fuzzy_index("title")
            products.create_vector_index("embedding", 2, "cosine")

            lexical = products.search_all(
                mode="lexical", query="coffee", fields=["title"], limit=10
            )
            assert [doc["_id"] for doc in lexical] == ["a", "b"], lexical
            assert all("_score" in doc and "_sources" in doc for doc in lexical)
            assert "lexical:title" in lexical[0]["_sources"]

            filtered = products.search_all(
                mode="lexical",
                query="coffee",
                fields=["title"],
                filter={"status": "active"},
                limit=10,
            )
            assert [doc["_id"] for doc in filtered] == ["a", "b"]
            paginated = products.search_all(
                mode="lexical",
                query="coffee",
                fields=["title"],
                skip=1,
                limit=1,
            )
            assert [doc["_id"] for doc in paginated] == ["b"], paginated

            fuzzy = products.search_all(
                mode="fuzzy", query="cofee", fields=["title"], max_distance=2, limit=10
            )
            assert "a" in {doc["_id"] for doc in fuzzy}, fuzzy

            vector = products.search_all(
                mode="vector",
                vector=[1.0, 0.0],
                vector_field="embedding",
                limit=2,
            )
            assert [doc["_id"] for doc in vector] == ["a", "b"], vector
            assert all("_sources" in doc and "_score" in doc for doc in vector)

            hybrid = products.search_all(
                mode="hybrid",
                query="coffee",
                fields=["title"],
                vector=[1.0, 0.0],
                vector_field="embedding",
                limit=3,
            )
            assert {doc["_id"] for doc in hybrid} >= {"a", "b"}, hybrid
            top = next(doc for doc in hybrid if doc["_id"] == "a")
            assert "lexical:title" in top["_sources"]
            assert "vector:embedding" in top["_sources"]

            found = db.exec(
                {
                    "op": "aggregate",
                    "coll": "products",
                    "pipeline": [
                        {"$search": {"text": {"query": "coffee", "path": "title"}}},
                        {"$limit": 1},
                        {"$project": {"_id": 1, "_score": 1}},
                    ],
                }
            )["results"]
            assert len(found) == 1 and found[0]["_score"] > 0, found

            meta = db.exec(
                {
                    "op": "aggregate",
                    "coll": "products",
                    "pipeline": [
                        {
                            "$searchMeta": {
                                "search": {
                                    "text": {"query": "coffee", "path": "title"}
                                }
                            }
                        }
                    ],
                }
            )["results"]
            assert meta[0]["count"]["total"] == 2, meta

            ranged = db.exec(
                {
                    "op": "aggregate",
                    "coll": "products",
                    "pipeline": [
                        {
                            "$search": {
                                "range": {"path": "price", "gte": 20, "lte": 30}
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ],
                }
            )["results"]
            assert [doc["_id"] for doc in ranged] == ["a", "b"], ranged

            vector_stage = db.exec(
                {
                    "op": "aggregate",
                    "coll": "products",
                    "pipeline": [
                        {
                            "$vectorSearch": {
                                "path": "embedding",
                                "queryVector": [1.0, 0.0],
                                "limit": 1,
                                "numCandidates": 10,
                            }
                        }
                    ],
                }
            )["results"]
            assert len(vector_stage) == 1, vector_stage
            assert vector_stage[0]["_id"] == "a", vector_stage
            assert vector_stage[0]["vectorSearchScore"] > 0

            try:
                db.exec(
                    {
                        "op": "aggregate",
                        "coll": "products",
                        "pipeline": [{"$search": {"autocomplete": {}}}],
                    }
                )
            except NonoDBError as error:
                assert "unsupported $search operator" in str(error), error
            else:
                raise AssertionError("unsupported $search operator was accepted")

            cli = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))
            if cli.is_file():
                cli_env = os.environ.copy()
                cli_env["NONODB_SOCK"] = str(sock)
                result = subprocess.run(
                    [
                        str(cli),
                        "search-all",
                        "products",
                        "--mode",
                        "lexical",
                        "--query",
                        "coffee",
                        "--fields",
                        "title",
                    ],
                    env=cli_env,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                assert result.returncode == 0, result.stderr
                assert "coffee beans" in result.stdout, result.stdout

            db.create_collection(
                "products_columnar",
                profile="columnar",
                columnar={"bucket_max_docs": 2, "compression": "max"},
            )
            columnar = db.collection("products_columnar")
            columnar.insert_many(
                [
                    {
                        "_id": "a",
                        "title": "coffee beans",
                        "description": "fresh roasted beans",
                        "status": "active",
                        "price": 20,
                        "embedding": [1.0, 0.0],
                    },
                    {
                        "_id": "b",
                        "title": "coffee grinder",
                        "description": "burr grinder",
                        "status": "active",
                        "price": 30,
                        "embedding": [0.9, 0.1],
                    },
                    {
                        "_id": "c",
                        "title": "tea",
                        "description": "green tea",
                        "status": "hidden",
                        "price": 5,
                        "embedding": [0.0, 1.0],
                    },
                ]
            )
            columnar.create_text_index("title")
            columnar.create_fuzzy_index("title")
            columnar.create_vector_index("embedding", 2, "cosine")
            columnar_lexical = columnar.search_all(
                mode="lexical", query="coffee", fields=["title"], limit=10
            )
            assert [doc["_id"] for doc in columnar_lexical] == ["a", "b"], (
                columnar_lexical
            )
            columnar_fuzzy = columnar.search_all(
                mode="fuzzy",
                query="cofee",
                fields=["title"],
                max_distance=2,
                limit=10,
            )
            assert "a" in {doc["_id"] for doc in columnar_fuzzy}, columnar_fuzzy
            columnar_vector = columnar.search_all(
                mode="vector",
                vector=[1.0, 0.0],
                vector_field="embedding",
                limit=2,
            )
            assert [doc["_id"] for doc in columnar_vector] == ["a", "b"], (
                columnar_vector
            )
            columnar_hybrid = columnar.search_all(
                mode="hybrid",
                query="coffee",
                fields=["title"],
                vector=[1.0, 0.0],
                vector_field="embedding",
                limit=3,
            )
            assert {doc["_id"] for doc in columnar_hybrid} >= {"a", "b"}, (
                columnar_hybrid
            )
            columnar_found = db.exec(
                {
                    "op": "aggregate",
                    "coll": "products_columnar",
                    "pipeline": [
                        {"$search": {"text": {"query": "coffee", "path": "title"}}},
                        {"$limit": 1},
                        {"$project": {"_id": 1, "_score": 1}},
                    ],
                }
            )["results"]
            assert len(columnar_found) == 1 and columnar_found[0]["_score"] > 0, (
                columnar_found
            )
            columnar_meta = db.exec(
                {
                    "op": "aggregate",
                    "coll": "products_columnar",
                    "pipeline": [
                        {
                            "$searchMeta": {
                                "search": {
                                    "text": {"query": "coffee", "path": "title"}
                                }
                            }
                        }
                    ],
                }
            )["results"]
            assert columnar_meta[0]["count"]["total"] == 2, columnar_meta
            columnar_vector_stage = db.exec(
                {
                    "op": "aggregate",
                    "coll": "products_columnar",
                    "pipeline": [
                        {
                            "$vectorSearch": {
                                "path": "embedding",
                                "queryVector": [1.0, 0.0],
                                "limit": 1,
                                "numCandidates": 10,
                            }
                        }
                    ],
                }
            )["results"]
            assert columnar_vector_stage[0]["_id"] == "a", columnar_vector_stage

            db.close()

            # Verify the same stages through the MongoDB wire aggregation path.
            try:
                from pymongo import MongoClient
            except ImportError:
                MongoClient = None
            if MongoClient is not None:
                mongo = MongoClient(
                    f"mongodb://127.0.0.1:{mongo_port}/?directConnection=true",
                    serverSelectionTimeoutMS=5_000,
                )
                wire_db = mongo.get_database("default")
                wire_docs = list(
                    wire_db.products.aggregate(
                        [
                            {"$search": {"text": {"query": "coffee", "path": "title"}}},
                            {"$limit": 1},
                        ]
                    )
                )
                assert len(wire_docs) == 1, wire_docs
                wire_meta = list(
                    wire_db.products.aggregate(
                        [
                            {
                                "$searchMeta": {
                                    "search": {
                                        "text": {"query": "coffee", "path": "title"}
                                    }
                                }
                            }
                        ]
                    )
                )
                assert wire_meta[0]["count"]["total"] == 2, wire_meta

                wire_range = list(
                    wire_db.products.aggregate(
                        [
                            {
                                "$search": {
                                    "range": {
                                        "path": "price",
                                        "gte": 20,
                                        "lte": 30,
                                    }
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    )
                )
                assert [doc["_id"] for doc in wire_range] == ["a", "b"], wire_range

                wire_vector = list(
                    wire_db.products.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0],
                                    "limit": 1,
                                    "numCandidates": 10,
                                }
                            }
                        ]
                    )
                )
                assert len(wire_vector) == 1, wire_vector
                assert wire_vector[0]["_id"] == "a", wire_vector

                wire_fuzzy = list(
                    wire_db.products.aggregate(
                        [
                            {
                                "$search": {
                                    "text": {
                                        "query": "cofee",
                                        "path": "title",
                                        "fuzzy": {"maxEdits": 0},
                                    }
                                }
                            }
                        ]
                    )
                )
                assert wire_fuzzy == [], wire_fuzzy

                try:
                    list(
                        wire_db.products.aggregate(
                            [
                                {
                                    "$vectorSearch": {
                                        "path": "embedding",
                                        "queryVector": [1.0, 0.0],
                                        "limit": 5,
                                        "numCandidates": 2,
                                    }
                                }
                            ]
                        )
                    )
                except Exception as error:
                    assert "numCandidates" in str(error), error
                else:
                    raise AssertionError("wire numCandidates < limit was accepted")
                mongo.close()
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            log.close()

    print("search test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
