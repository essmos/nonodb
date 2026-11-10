"""E2E test: Extended JSON dates, filtering, indexes, sorting, and errors.

Run against a daemon:

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/dates_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

COLL = "dates_e2e"


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
    c.drop_index("tenant+at")
    c.delete({}, multi=True)

    assert c.insert(
        {
            "_id": "utc",
            "tenant": "acme",
            "at": {"$date": "2026-01-01T00:00:00Z"},
        }
    ) == "utc"
    assert c.insert(
        {
            "_id": "warsaw",
            "tenant": "acme",
            "at": {"$date": "2026-01-01T01:00:00+01:00"},
        }
    ) == "warsaw"
    assert c.insert(
        {
            "_id": "later",
            "tenant": "acme",
            "at": {"$date": {"$numberLong": "1767229200000"}},
        }
    ) == "later"
    assert c.insert(
        {
            "_id": "other",
            "tenant": "other",
            "at": {"$date": "2026-01-03T00:00:00Z"},
        }
    ) == "other"

    # Bulk import recognizes dates too.
    imported = db.collection("dates_import_e2e")
    imported.delete({}, multi=True)
    imported.insert_many(
        [
            {"_id": "i1", "at": {"$date": "2026-02-01T00:00:00Z"}},
            {"_id": "i2", "at": {"$date": "2026-02-02T00:00:00+01:00"}},
        ]
    )
    got = imported.find(
        {"at": {"$gte": {"$date": "2026-02-01T00:00:00Z"}}},
        sort=[("at", 1)],
    )
    assert [d["_id"] for d in got] == ["i1", "i2"], got

    # Stored values are canonical Extended JSON.
    doc = c.find_one({"_id": "warsaw"})
    assert doc["at"] == {"$date": {"$numberLong": "1767225600000"}}, doc

    # Offsets for the same instant compare equal.
    same = c.find({"at": {"$date": "2025-12-31T19:00:00-05:00"}})
    assert {d["_id"] for d in same} == {"utc", "warsaw"}, same

    # Range filtering is by instant.
    rng = {
        "at": {
            "$gte": {"$date": "2026-01-01T01:00:00+01:00"},
            "$lt": {"$date": "2026-01-01T01:00:00Z"},
        }
    }
    assert [d["_id"] for d in c.find(rng)] == ["utc", "warsaw"], c.find(rng)

    # A normal index, including a compound one, serves date ranges.
    c.create_index(fields=["tenant", "at"])
    compound = {"tenant": "acme", "at": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}}
    assert [d["_id"] for d in c.find(compound, sort=[("at", 1)])] == [
        "utc",
        "warsaw",
        "later",
    ]
    assert c.count(compound) == 3

    # Date and string values are distinct types.
    c.insert({"_id": "string", "tenant": "acme", "at": "2026-01-01T00:00:00Z"})
    assert c.find({"at": {"$date": "2026-01-01T00:00:00Z"}})
    assert all(d["_id"] != "string" for d in c.find({"at": {"$date": "2026-01-01T00:00:00Z"}}))

    # Unique date indexes use the instant.
    uc = db.collection("dates_unique_e2e")
    uc.delete({}, multi=True)
    uc.create_index("at", unique=True)
    uc.insert({"_id": "first", "at": {"$date": "2026-01-01T00:00:00Z"}})
    expect(
        "duplicate_key",
        lambda: uc.insert(
            {"_id": "dupe", "at": {"$date": "2026-01-01T01:00:00+01:00"}}
        ),
    )

    expect(
        "invalid_date",
        lambda: c.insert({"_id": "naive", "at": {"$date": "2026-01-01T00:00:00"}}),
    )
    assert c.find_one({"_id": "naive"}) is None

    print("OK: dates, offsets, ranges, indexes, sorting, unique, invalid_date")
    db.close()


if __name__ == "__main__":
    main()
