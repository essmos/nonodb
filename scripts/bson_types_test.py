"""E2E test: Extended JSON integers, $type, binary/UUID, and ObjectId.

Run against a daemon:

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/bson_types_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

COLL = "bson_types_e2e"
UUID = "00112233-4455-6677-8899-aabbccddeeff"
UUID_B64 = "ABEiM0RVZneImaq7zN3u/w=="


def expect(code: str, fn) -> None:
    try:
        fn()
    except NonoDBError as exc:
        assert exc.code == code, f"expected {code}, got {exc.code}: {exc}"
        return
    raise AssertionError(f"expected error {code}, operation succeeded")


def binary(base64: str, subtype: str = "00") -> dict:
    return {"$binary": {"base64": base64, "subType": subtype}}


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
    c = db.collection(COLL)
    for field in ("n", "v"):
        c.drop_index(field)
    c.delete({}, multi=True)

    # Extended JSON integer forms keep their type and exact value.
    c.insert({"_id": "int", "n": {"$numberInt": "42"}})
    c.insert({"_id": "long", "n": {"$numberLong": "9007199254740993"}})
    c.insert({"_id": "double", "n": {"$numberDouble": "1.5"}})
    c.insert({"_id": "decimal", "n": {"$numberDecimal": "1.50"}})
    assert c.find_one({"_id": "long"})["n"] == {"$numberLong": "9007199254740993"}
    assert [d["_id"] for d in c.find({"n": {"$type": "int"}})] == ["int"]
    assert [d["_id"] for d in c.find({"n": {"$type": "long"}})] == ["long"]
    assert [d["_id"] for d in c.find({"n": {"$type": "double"}})] == ["double"]
    assert [d["_id"] for d in c.find({"n": {"$type": "decimal"}})] == ["decimal"]
    assert len(c.find({"n": {"$type": "number"}})) == 4

    # numberLong participates in exact indexes and $inc.
    lc = db.collection("bson_types_long_e2e")
    lc.drop_index("n")
    lc.delete({}, multi=True)
    lc.create_index("n")
    for n in ("9007199254740992", "9007199254740993", "9007199254740994"):
        lc.insert({"_id": n, "n": {"$numberLong": n}})
    rng = {"n": {"$gte": {"$numberLong": "9007199254740993"}}}
    assert [d["_id"] for d in lc.find(rng)] == [
        "9007199254740993",
        "9007199254740994",
    ]
    assert lc.count(rng) == 2
    lc.update(
        {"_id": "9007199254740993"},
        {"$inc": {"n": {"$numberLong": "1"}}},
    )
    assert lc.find_one({"_id": "9007199254740993"})["n"] == {
        "$numberLong": "9007199254740994"
    }

    # UUID input is canonicalized to binary subtype 4.
    bc = db.collection("bson_types_binary_e2e")
    bc.drop_index("v")
    bc.delete({}, multi=True)
    bc.create_index("v", unique=True)
    bc.insert({"_id": "uuid", "v": {"$uuid": UUID}})
    bc.insert({"_id": "a", "v": binary("YQ==")})
    bc.insert({"_id": "b", "v": binary("Yg==")})
    stored = bc.find_one({"_id": "uuid"})["v"]
    assert stored == binary(UUID_B64, "04"), stored
    assert [d["_id"] for d in bc.find({"v": {"$uuid": UUID}})] == ["uuid"]
    assert len(bc.find({"v": {"$type": "binData"}})) == 3
    expect(
        "duplicate_key",
        lambda: bc.insert({"_id": "dupe", "v": {"$uuid": UUID}}),
    )

    # ObjectId values: canonical form, queries, indexes, sorting.
    oc = db.collection("bson_types_oid_e2e")
    oc.drop_index("v")
    oc.delete({}, multi=True)
    oc.create_index("v")
    for _id, oid in (
        ("a", "000000000000000000000001"),
        ("b", "000000000000000000000002"),
        ("c", "000000000000000000000003"),
    ):
        oc.insert({"_id": _id, "v": {"$oid": oid}})
    assert [d["_id"] for d in oc.find({"v": {"$oid": "000000000000000000000002"}})] == [
        "b"
    ]
    assert len(oc.find({"v": {"$type": "objectId"}})) == 3
    assert [d["_id"] for d in oc.find({}, sort=[("v", -1)])] == ["c", "b", "a"]

    # $type handles arrays, codes, and aliases.
    tc = db.collection("bson_types_type_e2e")
    tc.delete({}, multi=True)
    tc.insert({"_id": "arr", "v": ["x", {"$numberInt": "1"}]})
    tc.insert({"_id": "s", "v": "x"})
    assert [d["_id"] for d in tc.find({"v": {"$type": "array"}})] == ["arr"]
    assert [d["_id"] for d in tc.find({"v": {"$type": "string"}})] == ["arr", "s"]
    assert [d["_id"] for d in tc.find({"v": {"$type": 16}})] == ["arr"]

    # Full-text/fuzzy indexes cover Extended JSON numbers and skip non-text types.
    sc = db.collection("bson_types_search_e2e")
    sc.drop_text_index("v")
    sc.drop_fuzzy_index("v")
    sc.delete({}, multi=True)
    sc.insert({"_id": "long", "v": {"$numberLong": "9007199254740993"}})
    sc.insert({"_id": "decimal", "v": {"$numberDecimal": "199.99"}})
    sc.insert({"_id": "nan", "v": {"$numberDouble": "NaN"}})
    sc.insert({"_id": "date", "v": {"$date": {"$numberLong": "1767225600000"}}})
    sc.insert({"_id": "oid", "v": {"$oid": "000000000000000000000001"}})
    sc.insert({"_id": "bin", "v": binary("YQ==")})
    sc.create_text_index("v")
    sc.create_fuzzy_index("v")
    assert [d["_id"] for d in sc.search("v", "9007199254740993")] == ["long"]
    assert [d["_id"] for d in sc.search("v", "199.99")] == ["decimal"]
    assert [d["_id"] for d in sc.search("v", "nan")] == ["nan"]
    assert sc.search("v", "1767225600000") == []
    assert sc.search("v", "000000000000000000000001") == []
    hits = sc.fuzzy("v", "9007199254740994", max_distance=1)
    assert [d["_id"] for d in hits] == ["long"], hits
    assert hits[0]["_distance"] == 1
    sc.drop_text_index("v")
    sc.drop_fuzzy_index("v")

    expect(
        "invalid_binary",
        lambda: c.insert({"_id": "bad_bin", "v": binary("!!!")}),
    )
    expect(
        "invalid_binary",
        lambda: c.insert({"_id": "bad_uuid", "v": {"$uuid": "nope"}}),
    )
    expect(
        "invalid_object_id",
        lambda: c.insert({"_id": "bad_oid", "v": {"$oid": "xyz"}}),
    )
    assert c.find_one({"_id": "bad_bin"}) is None

    print("OK: numberInt/numberLong, $type, binary/UUID, ObjectId")
    db.close()


if __name__ == "__main__":
    main()
