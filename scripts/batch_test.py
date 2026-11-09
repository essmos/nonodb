"""Test `/v1/batch`: wiele odczytów w jednej transakcji + zgodność z pojedynczymi.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/batch_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")


def main() -> int:
    db = NonoDB(SOCK, cache_ttl=0.0)
    c = db.collection("batchtest")
    c.delete({}, multi=True)
    c.drop_text_index("body")
    c.drop_fuzzy_index("title")

    ids = [c.insert({"n": i, "grp": i % 5, "title": f"Title {i}", "body": f"text {i}"}) for i in range(100)]
    c.create_index("grp")
    c.create_text_index("body")
    c.create_fuzzy_index("title")

    ops = [
        {"op": "find_one", "coll": c.name, "filter": {"_id": ids[7]}},
        {"op": "find", "coll": c.name, "filter": {"grp": 2}, "limit": 3},
        {"op": "count", "coll": c.name, "filter": {}},
        {"op": "count", "coll": c.name, "filter": {"grp": 1}},
        {"op": "list_indexes", "coll": c.name},
        {"op": "list_text_indexes", "coll": c.name},
        {"op": "list_fuzzy_indexes", "coll": c.name},
        {"op": "search", "coll": c.name, "field": "body", "query": "text 42"},
        {"op": "fuzzy", "coll": c.name, "field": "title", "query": "Titel 42", "max_distance": 2},
        {"op": "list_collections"},
        {"op": "find", "coll": "missing_collection", "filter": {}},
        {"op": "count", "coll": "missing_collection", "filter": {}},
    ]
    res = db.batch(ops)
    assert len(res) == len(ops)
    assert all(r["ok"] for r in res), res

    # Zgodność z pojedynczymi wywołaniami.
    assert res[0]["result"] == c.find_one({"_id": ids[7]})
    assert res[1]["result"] == c.find({"grp": 2}, limit=3)
    assert res[2]["result"]["count"] == 100
    assert res[3]["result"]["count"] == 20
    assert res[4]["result"]["indexes"] == c.list_indexes()
    assert res[5]["result"]["text_indexes"] == ["body"]
    assert res[6]["result"]["fuzzy_indexes"] == ["title"]
    assert res[7]["result"]["results"][0]["_id"] == ids[42]
    assert res[8]["result"]["results"][0]["_id"] == ids[42]
    assert c.name in res[9]["result"]["collections"]
    assert res[10]["result"] == []
    assert res[11]["result"]["count"] == 0

    # Zapis w batchu jest odrzucany per-operacja, bez przerywania całości.
    res = db.batch(
        [
            {"op": "insert", "coll": c.name, "doc": {"x": 1}},
            {"op": "count", "coll": c.name, "filter": {}},
        ]
    )
    assert res[0]["ok"] is False and res[0]["error"]["code"] == "invalid_query"
    assert res[1]["ok"] is True and res[1]["result"]["count"] == 100

    # get_many: jedno żądanie, wiele dokumentów.
    got = c.get_many(ids[:10])
    assert len(got) == 10
    assert {d["_id"] for d in got} == set(ids[:10])

    # Pusty batch.
    assert db.batch([]) == []

    # Cache obejmuje batch i jest unieważniany zapisem.
    cached = NonoDB(SOCK, cache_ttl=5.0)
    cc = cached.collection(c.name)
    before = cached.batch([{"op": "count", "coll": c.name, "filter": {}}])[0]["result"]["count"]
    cc.insert({"n": 999})
    after = cached.batch([{"op": "count", "coll": c.name, "filter": {}}])[0]["result"]["count"]
    assert before == 100 and after == 101, (before, after)
    cached.close()

    print("OK: batch (wszystkie odczyty, błędy per-op, get_many, cache, pusta lista)")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
