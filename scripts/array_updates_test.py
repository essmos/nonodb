"""E2E test: MongoDB-style array updates.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/array_updates_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

COLL = "array_updates_e2e"


def expect(code: str, fn) -> None:
    try:
        fn()
    except NonoDBError as exc:
        assert exc.code == code, f"expected {code}, got {exc.code}: {exc}"
        return
    raise AssertionError(f"expected error {code}, operation succeeded")


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
    c = db.collection(COLL)
    c.delete({}, multi=True)
    c.insert(
        {
            "_id": "a",
            "items": [
                {"sku": "x", "qty": 2},
                {"sku": "y", "qty": 9},
                {"sku": "x", "qty": 10},
            ],
            "tags": ["b"],
            "nums": [1, 5, 10],
        }
    )

    c.update(
        {"items": {"$elemMatch": {"sku": "y"}}},
        {"$set": {"items.$.qty": 5}},
    )
    assert c.find_one({"_id": "a"})["items"][1]["qty"] == 5

    c.update({"_id": "a"}, {"$inc": {"items.$[].qty": 1}})
    doc = c.find_one({"_id": "a"})
    assert [i["qty"] for i in doc["items"]] == [3, 6, 11]

    c.update(
        {"_id": "a"},
        {"$set": {"items.$[i].checked": True}},
        array_filters=[{"i.sku": "x", "i.qty": {"$lt": 5}}],
    )
    doc = c.find_one({"_id": "a"})
    assert doc["items"][0]["checked"] is True
    assert "checked" not in doc["items"][1]
    assert "checked" not in doc["items"][2]

    c.update(
        {"_id": "a"},
        {"$push": {"tags": {"$each": ["a", "c"], "$sort": 1, "$slice": 2}}},
    )
    assert c.find_one({"_id": "a"})["tags"] == ["a", "b"]

    c.update({"_id": "a"}, {"$pull": {"nums": {"$gte": 5}}})
    assert c.find_one({"_id": "a"})["nums"] == [1]

    c.update(
        {"_id": "a"},
        {"$push": {"nums": {"$each": [2, 3], "$position": 1}}},
    )
    assert c.find_one({"_id": "a"})["nums"] == [1, 2, 3]

    c.update({"_id": "a"}, {"$pullAll": {"nums": [2]}})
    assert c.find_one({"_id": "a"})["nums"] == [1, 3]

    expect(
        "invalid_query",
        lambda: c.update({"_id": "a"}, {"$push": {"tags": {"$position": 1}}}),
    )
    expect(
        "invalid_query",
        lambda: c.update(
            {"_id": "a"},
            {"$set": {"items.$[i].qty": 1}},
            array_filters=[{"j.sku": "x"}],
        ),
    )
    expect(
        "invalid_query",
        lambda: c.update(
            {"_id": "a"},
            {"$set": {"items.$[i].qty": 1}},
            array_filters=[{}],
        ),
    )
    assert c.find_one({"_id": "a"})["items"][0]["qty"] == 3

    # Numeric array indexes can be written directly; out-of-range assignment pads null.
    c.update({"_id": "a"}, {"$set": {"items.0.qty": 42}})
    assert c.find_one({"_id": "a"})["items"][0]["qty"] == 42
    c.update({"_id": "a"}, {"$set": {"nums.5": 9}})
    assert c.find_one({"_id": "a"})["nums"] == [1, 3, None, None, None, 9]

    print("array updates OK")


if __name__ == "__main__":
    main()
