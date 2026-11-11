"""E2E test: array operators $all, $size, and $elemMatch.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/arrays_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

COLL = "arrays_e2e"


def expect(code: str, fn) -> None:
    try:
        fn()
    except NonoDBError as exc:
        assert exc.code == code, f"expected {code}, got {exc.code}: {exc}"
        return
    raise AssertionError(f"expected error {code}, operation succeeded")


def ids(docs: list[dict]) -> list[str]:
    return [d["_id"] for d in docs]


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
    c = db.collection(COLL)
    c.drop_index("kind")
    c.delete({}, multi=True)

    c.insert(
        {
            "_id": "a",
            "kind": "x",
            "tags": ["rust", "lmdb"],
            "nums": [1, 2, 3],
            "items": [{"sku": "x", "qty": 2}, {"sku": "y", "qty": 9}],
        }
    )
    c.insert(
        {
            "_id": "b",
            "kind": "y",
            "tags": ["rust"],
            "nums": [5],
            "items": [{"sku": "x", "qty": 10}],
        }
    )
    c.insert(
        {
            "_id": "c",
            "kind": "x",
            "tags": "rust",
            "nums": [],
            "items": [],
        }
    )

    assert ids(c.find({"tags": {"$all": ["rust", "lmdb"]}})) == ["a"]
    assert ids(c.find({"tags": {"$all": ["rust"]}})) == ["a", "b", "c"]
    assert c.find({"tags": {"$all": []}}) == []
    assert ids(c.find({"nums": {"$size": 3}})) == ["a"]
    assert ids(c.find({"nums": {"$size": 0}})) == ["c"]
    assert ids(c.find({"nums": {"$elemMatch": {"$gte": 2, "$lt": 4}}})) == ["a"]
    assert ids(
        c.find({"items": {"$elemMatch": {"sku": "x", "qty": {"$gt": 5}}}})
    ) == ["b"]
    assert ids(
        c.find(
            {
                "items": {
                    "$elemMatch": {"$and": [{"sku": "y"}, {"qty": 9}]}
                }
            }
        )
    ) == ["a"]
    assert len(c.find({"items": {"$elemMatch": {}}})) == 2
    assert ids(c.find({"tags": {"$not": {"$size": 2}}})) == ["b", "c"]

    # A single-value $all is equivalent to equality and can use an index.
    c.create_index("kind")
    assert ids(c.find({"kind": {"$all": ["x"]}})) == ["a", "c"]
    assert c.count({"kind": {"$all": ["x"]}}) == 2
    c.drop_index("kind")

    # The same filter operators work in aggregation and search filters.
    out = c.aggregate(
        [{"$match": {"tags": {"$all": ["rust"]}}}, {"$count": "n"}]
    )
    assert out == [{"n": 3}], out
    c.create_text_index("kind")
    assert ids(c.search("kind", "x", filter={"nums": {"$size": 3}})) == ["a"]
    c.drop_text_index("kind")

    expect("invalid_query", lambda: c.find({"tags": {"$all": "rust"}}))
    expect("invalid_query", lambda: c.find({"nums": {"$size": -1}}))
    expect("invalid_query", lambda: c.find({"nums": {"$size": 1.5}}))
    expect("invalid_query", lambda: c.find({"nums": {"$elemMatch": 1}}))
    expect(
        "invalid_query",
        lambda: c.update(
            {"nums": {"$size": "3"}},
            {"$set": {"bad": True}},
            multi=True,
        ),
    )
    assert c.find_one({"_id": "a"}).get("bad") is None

    c.delete({}, multi=True)
    print("OK: $all, $size, $elemMatch, validation, index/aggregation/search")
    db.close()


if __name__ == "__main__":
    main()
