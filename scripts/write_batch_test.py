"""Test atomowego `write_batch`: sukces, rollback, wiele kolekcji."""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")


def main() -> None:
    db = NonoDB(SOCK, timeout=60)
    a = db.collection("wb_a")
    b = db.collection("wb_b")
    a.delete({}, multi=True)
    b.delete({}, multi=True)

    # 1) udany batch: insert + insert_many + update + delete
    res = db.write_batch(
        [
            {"op": "insert", "coll": a.name, "doc": {"_id": "a1", "v": 1}},
            {
                "op": "insert_many",
                "coll": a.name,
                "docs": [{"_id": "a2", "v": 2}, {"_id": "a3", "v": 3}],
            },
            {
                "op": "update",
                "coll": a.name,
                "filter": {"_id": "a2"},
                "update": {"$set": {"v": 20}},
            },
            {"op": "delete", "coll": a.name, "filter": {"_id": "a3"}},
        ]
    )
    assert len(res) == 4, res
    assert a.count({}) == 2, a.count({})
    assert a.find_one({"_id": "a2"})["v"] == 20

    # 2) rollback całego batcha po duplikacie `_id`
    try:
        db.write_batch(
            [
                {"op": "insert", "coll": a.name, "doc": {"_id": "a4"}},
                {"op": "insert", "coll": a.name, "doc": {"_id": "a1"}},  # duplikat
                {"op": "insert", "coll": a.name, "doc": {"_id": "a5"}},
            ]
        )
    except NonoDBError as exc:
        assert exc.code == "duplicate_key", exc
    else:
        raise AssertionError("oczekiwano duplicate_key")
    assert a.find_one({"_id": "a4"}) is None, "rollback nie usunął a4"
    assert a.find_one({"_id": "a5"}) is None, "rollback nie usunął a5"
    assert a.count({}) == 2

    # 3) rollback obejmuje wiele kolekcji
    try:
        db.write_batch(
            [
                {"op": "insert", "coll": a.name, "doc": {"_id": "a6"}},
                {"op": "insert", "coll": b.name, "doc": {"_id": "b1"}},
                {"op": "insert", "coll": b.name, "doc": {"_id": "b1"}},  # duplikat
            ]
        )
    except NonoDBError as exc:
        assert exc.code == "duplicate_key", exc
    else:
        raise AssertionError("oczekiwano duplicate_key")
    assert a.find_one({"_id": "a6"}) is None
    assert b.count({}) == 0

    # 4) batch odczytowy nadal odrzuca zapisy per-operacja
    res = db.batch(
        [
            {"op": "insert", "coll": a.name, "doc": {"_id": "bad"}},
            {"op": "count", "coll": a.name, "filter": {}},
        ]
    )
    assert res[0]["ok"] is False and res[0]["error"]["code"] == "invalid_query", res
    assert res[1]["ok"] is True and res[1]["result"]["count"] == 2, res

    a.delete({}, multi=True)
    b.delete({}, multi=True)
    db.close()
    print("OK: write_batch atomowy (sukces, rollback, wiele kolekcji, read batch)")


if __name__ == "__main__":
    main()
