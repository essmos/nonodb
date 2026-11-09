"""Test e2e: indeksy złożone, covering, count, frazy i folding diakrytyków.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/compound_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB  # noqa: E402


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
    c = db.collection("cmp")
    c.delete({}, multi=True)
    for f in c.list_indexes():
        c.drop_index(f["field"] if isinstance(f, dict) else f)
    for f in c.list_text_indexes():
        c.drop_text_index(f)

    for g in range(4):
        for age in range(5):
            c.insert(
                {
                    "_id": f"g{g}a{age}",
                    "grp": g,
                    "age": age,
                    "score": g * 10 + age,
                    "bio": "Zażółć gęślą jaźń" if age % 2 else "Ala ma kota",
                }
            )

    c.create_index(fields=["grp", "age"], include=["score"])
    idx = c.list_indexes()
    assert any(i["fields"] == ["grp", "age"] and i["include"] == ["score"] for i in idx), idx

    # Sort rosnący/malejący dokładnie po indeksie.
    got = c.find({"grp": {"$gte": 1}}, sort=[["grp", 1], ["age", 1]], limit=4)
    assert [(d["grp"], d["age"]) for d in got] == [(1, 0), (1, 1), (1, 2), (1, 3)], got
    got = c.find({"grp": {"$gte": 1}}, sort=[["grp", -1], ["age", -1]], limit=4)
    assert [(d["grp"], d["age"]) for d in got] == [(3, 4), (3, 3), (3, 2), (3, 1)], got

    # Zakres na ostatnim polu + count bez JSON-a.
    got = c.find({"grp": 2, "age": {"$gte": 2}}, sort=[["grp", 1], ["age", 1]])
    assert [d["age"] for d in got] == [2, 3, 4], got
    assert c.count({"grp": 2, "age": {"$gte": 2}}) == 3
    assert c.count({"grp": 2, "age": {"$gte": 2}, "bio": "Ala ma kota"}) == 2

    # Covering: projekcja z wartości indeksu.
    got = c.find(
        {"grp": 2},
        projection={"grp": 1, "age": 1, "score": 1, "_id": 0},
        sort=[["grp", 1], ["age", 1]],
        limit=2,
    )
    assert got == [{"grp": 2, "age": 0, "score": 20}, {"grp": 2, "age": 1, "score": 21}], got

    # Text index: folding + fraza.
    c.create_text_index("bio")
    assert c.search("bio", "zazolc", limit=10)
    assert len(c.search("bio", '"ala ma"', limit=10)) == 10
    assert c.search("bio", '"ma ala"', limit=10) == []

    # Atomiczny write_batch.
    res = db.write_batch(
        [
            {"op": "insert", "coll": c.name, "doc": {"_id": "x", "grp": 9, "age": 9}},
            {"op": "delete", "coll": c.name, "filter": {"_id": "x"}},
        ]
    )
    assert len(res) == 2 and c.find_one({"_id": "x"}) is None

    print("OK: compound index + covering + count + frazy/folding + write_batch")
    db.close()


if __name__ == "__main__":
    main()
