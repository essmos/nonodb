"""E2E test: exact numbers, Decimal128, special doubles, and indexes.

Run against a daemon:

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/numerics_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

COLL = "numerics_e2e"


def expect(code: str, fn) -> None:
    try:
        fn()
    except NonoDBError as exc:
        assert exc.code == code, f"expected {code}, got {exc.code}: {exc}"
        return
    raise AssertionError(f"expected error {code}, operation succeeded")


def dec(value: str) -> dict:
    return {"$numberDecimal": value}


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
    c = db.collection(COLL)
    c.drop_index("n")
    c.drop_index("price")
    c.delete({}, multi=True)

    # Large integers must not be rounded through f64.
    for n in (9007199254740992, 9007199254740993, 9007199254740994):
        c.insert({"_id": str(n), "n": n})
    got = c.find({"n": 9007199254740993})
    assert [d["_id"] for d in got] == ["9007199254740993"], got
    got = c.find({"n": {"$gte": 9007199254740993}})
    assert {d["_id"] for d in got} == {"9007199254740993", "9007199254740994"}, got

    # Decimal and ordinary numbers share one numeric bracket.
    pc = db.collection("numerics_prices_e2e")
    pc.drop_index("price")
    pc.delete({}, multi=True)
    pc.insert({"_id": "int", "price": 1})
    pc.insert({"_id": "float", "price": 1.0})
    pc.insert({"_id": "dec", "price": dec("1.00")})
    pc.insert({"_id": "higher", "price": dec("1.50")})
    assert {d["_id"] for d in pc.find({"price": dec("1")})} == {
        "int",
        "float",
        "dec",
    }
    assert [d["_id"] for d in pc.find({"price": {"$gt": dec("1.00")}})] == [
        "higher"
    ]

    # Decimal range, index, sort, count, and unique constraint.
    pc.delete({}, multi=True)
    pc.create_index("price")
    for _id, price in (
        ("a", "10.00"),
        ("b", "20.50"),
        ("c", "99.99"),
        ("d", "5.00"),
    ):
        pc.insert({"_id": _id, "price": dec(price)})
    rng = {"price": {"$gte": dec("10"), "$lt": dec("100")}}
    assert [d["_id"] for d in pc.find(rng)] == ["a", "b", "c"], pc.find(rng)
    assert pc.count(rng) == 3
    assert [d["_id"] for d in pc.find({}, sort=[("price", -1)])] == [
        "c",
        "b",
        "a",
        "d",
    ]

    uc = db.collection("numerics_unique_e2e")
    uc.drop_index("price")
    uc.delete({}, multi=True)
    uc.create_index("price", unique=True)
    uc.insert({"_id": "first", "price": dec("1.00")})
    expect(
        "duplicate_key",
        lambda: uc.insert({"_id": "second", "price": dec("1.0")}),
    )

    # Decimal arithmetic through $inc, $sum, $avg.
    ac = db.collection("numerics_accounts_e2e")
    ac.delete({}, multi=True)
    ac.insert({"_id": "a", "balance": dec("10.10")})
    ac.insert({"_id": "b", "balance": dec("0.20")})
    ac.update({"_id": "a"}, {"$inc": {"balance": dec("0.20")}})
    got = ac.find_one({"_id": "a"})
    assert got["balance"] == dec("10.3"), got
    out = ac.aggregate(
        [{"$group": {"_id": None, "sum": {"$sum": "$balance"}, "avg": {"$avg": "$balance"}}}]
    )
    assert out[0]["sum"] == dec("10.5"), out
    assert out[0]["avg"]["$numberDecimal"].startswith("5.25"), out

    # Non-finite doubles are queryable, indexable, and sortable.
    nc = db.collection("numerics_doubles_e2e")
    nc.drop_index("n")
    nc.delete({}, multi=True)
    nc.create_index("n")
    nc.insert({"_id": "nan", "n": {"$numberDouble": "NaN"}})
    nc.insert({"_id": "neg_inf", "n": {"$numberDouble": "-Infinity"}})
    nc.insert({"_id": "zero", "n": 0})
    nc.insert({"_id": "pos_inf", "n": {"$numberDouble": "Infinity"}})
    assert [d["_id"] for d in nc.find({"n": {"$numberDouble": "NaN"}})] == ["nan"]
    assert [d["_id"] for d in nc.find({}, sort=[("n", 1)])] == [
        "nan",
        "neg_inf",
        "zero",
        "pos_inf",
    ]

    # Invalid Extended JSON is rejected before storage.
    expect(
        "invalid_numeric",
        lambda: c.insert({"_id": "bad", "price": dec("not-a-number")}),
    )
    expect(
        "invalid_numeric",
        lambda: c.insert({"_id": "nan_dec", "price": dec("NaN")}),
    )
    assert c.find_one({"_id": "bad"}) is None
    assert c.find_one({"_id": "nan_dec"}) is None

    print("OK: exact integers, Decimal128, arithmetic, special doubles, indexes")
    db.close()


if __name__ == "__main__":
    main()
