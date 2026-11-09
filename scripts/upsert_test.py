"""Test e2e: upsert, $setOnInsert, $addToSet/$each, $push/$each.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/upsert_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB  # noqa: E402


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
    c = db.collection("upsert_t")
    c.delete({}, multi=True)

    # Insert przez upsert: equalities z filtra + $setOnInsert + tablice.
    out = c.update_result(
        {"tenant": "a", "key": "mail"},
        {
            "$set": {"host": "smtp"},
            "$setOnInsert": {"created": True},
            "$addToSet": {"tags": {"$each": ["x", "x", "y"]}},
            "$push": {"ports": {"$each": [25, 587]}},
        },
        upsert=True,
    )
    assert out["matched"] == 0 and out["modified"] == 0, out
    assert out["upserted_id"], out
    doc = c.find_one({"_id": out["upserted_id"]})
    assert doc["tenant"] == "a" and doc["key"] == "mail", doc
    assert doc["host"] == "smtp" and doc["created"] is True, doc
    assert doc["tags"] == ["x", "y"], doc
    assert doc["ports"] == [25, 587], doc

    # Drugi upsert tylko aktualizuje: bez nowego _id, bez $setOnInsert.
    out = c.update_result(
        {"tenant": "a", "key": "mail"},
        {
            "$set": {"host": "smtp2"},
            "$setOnInsert": {"created": False},
            "$addToSet": {"tags": "z"},
            "$push": {"ports": 465},
        },
        upsert=True,
    )
    assert out["matched"] == 1 and out["modified"] == 1, out
    assert out["upserted_id"] is None, out
    doc = c.find_one({"_id": doc["_id"]})
    assert doc["host"] == "smtp2" and doc["created"] is True, doc
    assert doc["tags"] == ["x", "y", "z"], doc
    assert doc["ports"] == [25, 587, 465], doc

    # Replacement upsert także zachowuje equalities z filtra.
    out = c.update_result({"tenant": "b", "key": "db"}, {"value": 1}, upsert=True)
    doc = c.find_one({"_id": out["upserted_id"]})
    assert (doc["tenant"], doc["key"], doc["value"]) == ("b", "db", 1), doc

    # Upsert w atomowym write_batch.
    res = db.write_batch(
        [
            {
                "op": "update",
                "coll": c.name,
                "filter": {"tenant": "c", "key": "x"},
                "update": {"$set": {"v": 1}},
                "upsert": True,
            }
        ]
    )
    assert res[0]["upserted_id"], res
    assert c.count({}) == 3

    # Bez upsert brak trafień nic nie zmienia.
    out = c.update_result({"tenant": "none"}, {"$set": {"x": 1}})
    assert out == {"matched": 0, "modified": 0, "upserted_id": None}, out

    print("OK: upsert + $setOnInsert + $addToSet/$each + $push/$each + write_batch")
    db.close()


if __name__ == "__main__":
    main()
