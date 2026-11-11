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

    print("explain OK")


if __name__ == "__main__":
    main()
