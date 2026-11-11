"""E2E test: MongoDB-style $regex/$options filters.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/regex_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

COLL = "regex_e2e"


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
    c.drop_index("name")
    c.delete({}, multi=True)

    c.insert({"_id": "a", "name": "Ala", "tags": ["kot", "pies"], "n": 42})
    c.insert({"_id": "b", "name": "Alan", "tags": ["ptak"], "n": 7})
    c.insert({"_id": "c", "name": "Bob", "tags": ["pies"], "n": 99})
    c.insert({"_id": "date", "name": {"$date": {"$numberLong": "0"}}})

    assert ids(c.find({"name": {"$regex": "^Al"}})) == ["a", "b"]
    assert ids(c.find({"name": {"$regex": "^ala$", "$options": "i"}})) == ["a"]
    assert ids(c.find({"name": {"$regex": "^Bob$"}})) == ["c"]
    assert ids(c.find({"tags": {"$regex": "^pi"}})) == ["a", "c"]
    assert ids(c.find({"name": {"$not": {"$regex": "^Al"}}})) == ["c", "date"]
    assert c.count({"name": {"$regex": "^Al"}}) == 2
    assert c.find({"n": {"$regex": "42"}}) == []
    assert ids(
        c.find(
            {
                "name": {
                    "$regex": {
                        "$regularExpression": {
                            "pattern": "^ala$",
                            "options": "i",
                        }
                    }
                }
            }
        )
    ) == ["a"]
    assert ids(
        c.find(
            {
                "$or": [
                    {"name": {"$regex": "^Al"}},
                    {"name": {"$regex": "^Bo"}},
                ]
            }
        )
    ) == ["a", "b", "c"]

    # Anchored patterns can use a string-index prefix range.
    c.create_index("name")
    assert ids(c.find({"name": {"$regex": "^Al"}})) == ["a", "b"]
    assert c.count({"name": {"$regex": "^Al"}}) == 2
    c.drop_index("name")

    # The aggregation leading $match uses the same matcher.
    out = c.aggregate([{"$match": {"name": {"$regex": "^Al"}}}, {"$count": "n"}])
    assert out == [{"n": 2}], out

    # Invalid patterns/options are request errors and do not mutate data.
    expect("invalid_query", lambda: c.find({"name": {"$regex": "("}}))
    expect("invalid_query", lambda: c.find({"name": {"$regex": "x", "$options": "q"}}))
    expect("invalid_query", lambda: c.find({"name": {"$options": "i"}}))
    expect(
        "invalid_query",
        lambda: c.update({"name": {"$regex": "("}}, {"$set": {"x": 1}}, multi=True),
    )
    assert c.find_one({"_id": "a"}).get("x") is None
    assert c.count({}) == 4

    c.delete({}, multi=True)
    print("OK: $regex/$options, arrays, $not, extended form, index prefix, errors")
    db.close()


if __name__ == "__main__":
    main()
