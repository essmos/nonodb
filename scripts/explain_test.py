"""E2E test: query explain plans and counts.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/explain_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

COLL = "explain_e2e"


def expect_invalid(fn) -> None:
    try:
        fn()
    except NonoDBError as exc:
        assert exc.code == "invalid_query", exc
        return
    raise AssertionError("expected invalid_query")


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
    c = db.collection(COLL)
    c.delete({}, multi=True)
    c.insert_many(
        [
            {"_id": "a", "score": 1, "kind": "x"},
            {"_id": "b", "score": 2, "kind": "x"},
            {"_id": "c", "score": 3, "kind": "y"},
        ]
    )
    c.create_index("score")

    plan = c.explain({"score": {"$gte": 2}}, sort=[("score", 1)], limit=5)
    assert plan["access_path"] == "index_scan", plan
    assert plan["index_used"] is True, plan
    assert plan["index"]["fields"] == ["score"], plan
    assert plan["candidates"] == 2, plan
    assert plan["actual_matched"] == 2, plan
    assert plan["actual_returned"] == 2, plan
    assert plan["sort_covered"] is True, plan
    assert plan["bounds"]["lower"]["value"] is not None, plan

    plan = c.explain({"_id": "b"})
    assert plan["access_path"] == "id_lookup", plan
    assert plan["candidates"] == 1, plan
    assert plan["actual_matched"] == 1, plan

    plan = c.explain({"_id": {"$gte": "a", "$lt": "c"}})
    assert plan["access_path"] == "id_range", plan
    assert plan["candidates"] == 2, plan
    assert plan["bounds"]["lower"]["value"] == "a", plan
    assert plan["bounds"]["upper"]["inclusive"] is False, plan

    plan = c.explain({"kind": "x"}, sort=[("score", 1)], skip=1, limit=1)
    assert plan["access_path"] == "collection_scan", plan
    assert plan["index_used"] is False, plan
    assert plan["candidates"] == 3, plan
    assert plan["actual_matched"] == 2, plan
    assert plan["actual_returned"] == 1, plan
    assert plan["sort_covered"] is False, plan

    # Explain is cached, but a committed write invalidates the cached plan.
    c.insert({"_id": "d", "score": 4, "kind": "x"})
    plan = c.explain({"kind": "x"})
    assert plan["actual_matched"] == 3, plan

    expect_invalid(lambda: c.explain({"score": {"$all": 1}}))
    expect_invalid(lambda: c.explain({"score": {"$regex": "["}}))

    plan = db.exec(
        {
            "op": "explain_lookup",
            "coll": COLL,
            "by": "_id",
            "values": ["a", "missing", "a"],
        }
    )
    assert plan["collection"] == COLL, plan
    assert plan["selected_index"] == "_id_", plan
    assert plan["index"]["primary"] is True, plan
    assert plan["covering"] is False, plan
    assert plan["key_counts"] == {"requested": 3, "unique": 2, "duplicates": 1}, plan
    assert plan["found"] == 2, plan
    assert plan["missing"] == 1, plan
    assert plan["documents_read"] == 1, plan
    assert plan["bytes_read"] > 0 and plan["bytes_returned"] > 0, plan
    assert plan["elapsed_ms"] >= 0, plan

    plan = db.exec(
        {
            "op": "explain_lookup",
            "coll": COLL,
            "by": "_id",
            "values": ["a", "missing"],
            "projection": {"_id": 1},
        }
    )
    assert plan["covering"] is True, plan
    assert plan["documents_read"] == 0, plan
    assert plan["found"] == 1 and plan["missing"] == 1, plan

    unique = db.collection("explain_lookup_unique")
    unique.delete({}, multi=True)
    unique.insert_many(
        [
            {"_id": "u1", "email": "a@example.com", "name": "A"},
            {"_id": "u2", "email": "b@example.com", "name": "B"},
        ]
    )
    unique.create_index("email", unique=True, include=["name"])
    plan = db.exec(
        {
            "op": "explain_lookup",
            "coll": unique.name,
            "by": "email",
            "values": ["a@example.com", "missing@example.com", "a@example.com"],
            "fields": ["name"],
        }
    )
    assert plan["selected_index"] == "email", plan
    assert plan["index"]["fields"] == ["email"], plan
    assert plan["index"]["include"] == ["name"], plan
    assert plan["index"]["unique"] is True, plan
    assert plan["covering"] is True, plan
    assert plan["documents_read"] == 0, plan
    assert plan["key_counts"] == {"requested": 3, "unique": 2, "duplicates": 1}, plan
    assert plan["found"] == 2 and plan["missing"] == 1, plan

    expect_invalid(
        lambda: db.exec(
            {
                "op": "explain_lookup",
                "coll": COLL,
                "by": "kind",
                "values": ["x"],
            }
        )
    )

    # Lookup limits and projection validation apply to missing collections and
    # read batches exactly as they do to a normal lookup.
    expect_invalid(
        lambda: db.exec({"op": "lookup", "coll": COLL, "by": "", "values": ["a"]})
    )
    expect_invalid(
        lambda: db.exec(
            {"op": "lookup", "coll": COLL, "values": ["a"] * 10_001}
        )
    )

    missing = "explain_missing_lookup"
    ok = db.batch(
        [
            {
                "op": "lookup",
                "coll": missing,
                "by": "_id",
                "values": ["a", "b"],
            },
            {
                "op": "explain_lookup",
                "coll": missing,
                "by": "_id",
                "values": ["a", "b"],
            },
        ]
    )
    assert ok[0]["ok"] is True, ok[0]
    assert ok[0]["result"] == {"results": [None, None], "found": 0}, ok[0]
    assert ok[1]["ok"] is True, ok[1]
    assert ok[1]["result"]["missing"] == 2, ok[1]

    for op in ("lookup", "explain_lookup"):
        bad = db.batch(
            [
                {"op": op, "coll": missing, "by": "", "values": ["a"]},
                {
                    "op": op,
                    "coll": missing,
                    "values": ["a"] * 10_001,
                },
                {
                    "op": op,
                    "coll": missing,
                    "values": ["a"],
                    "fields": ["x"],
                    "projection": {"x": 1},
                },
            ]
        )
        assert [entry["ok"] for entry in bad] == [False, False, False], bad
        assert [entry["error"]["code"] for entry in bad] == [
            "invalid_query",
            "invalid_query",
            "invalid_query",
        ], bad

    print("explain OK")


if __name__ == "__main__":
    main()
