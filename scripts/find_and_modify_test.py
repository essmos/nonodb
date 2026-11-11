"""E2E test: atomic find-and-modify.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/find_and_modify_test.py
"""

from __future__ import annotations

import os
import sys
import threading

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

COLL = "find_and_modify_e2e"


def expect(code: str, fn) -> None:
    try:
        fn()
    except NonoDBError as exc:
        assert exc.code == code, f"expected {code}, got {exc.code}: {exc}"
        return
    raise AssertionError(f"expected error {code}, operation succeeded")


def main() -> None:
    sock = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
    db = NonoDB(sock, timeout=60)
    c = db.collection(COLL)
    c.delete({}, multi=True)
    c.insert_many(
        [
            {"_id": "a", "score": 1, "n": 1},
            {"_id": "b", "score": 9, "n": 1},
            {"_id": "c", "score": 5, "n": 1},
        ]
    )

    old = c.find_and_modify({"_id": "a"}, {"$inc": {"n": 1}})
    assert old == {"_id": "a", "score": 1, "n": 1}
    assert c.find_one({"_id": "a"})["n"] == 2

    new = c.find_and_modify(
        {},
        {"$set": {"picked": True}},
        new=True,
        sort=[("score", -1)],
        projection={"_id": 1, "score": 1, "picked": 1},
    )
    assert new == {"_id": "b", "score": 9, "picked": True}
    assert c.find_one({"_id": "b"})["picked"] is True
    assert "picked" not in c.find_one({"_id": "c"})

    # arrayFilters are honored by the atomic path.
    c.insert(
        {
            "_id": "arr",
            "items": [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 2}],
        }
    )
    got = c.find_and_modify(
        {"_id": "arr"},
        {"$inc": {"items.$[i].qty": 10}},
        new=True,
        array_filters=[{"i.sku": "y"}],
    )
    assert got["items"] == [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 12}]

    up = c.find_and_modify_result(
        {"_id": "upserted", "kind": "x"},
        {"$set": {"n": 1}},
        upsert=True,
        new=True,
    )
    assert up["upserted_id"] == "upserted"
    assert up["value"] == {"_id": "upserted", "kind": "x", "n": 1}

    removed = c.find_and_modify({"_id": "upserted"}, remove=True)
    assert removed == {"_id": "upserted", "kind": "x", "n": 1}
    assert c.find_one({"_id": "upserted"}) is None

    expect(
        "invalid_query",
        lambda: c.find_and_modify({"_id": "a"}, {"$set": {"x": 1}}, remove=True),
    )
    expect(
        "invalid_query",
        lambda: c.find_and_modify({"_id": "a"}, remove=True, upsert=True),
    )
    expect("invalid_query", lambda: c.find_and_modify({"_id": "a"}))
    expect(
        "invalid_query",
        lambda: c.find_and_modify({"_id": "arr"}, {"$set": {"items.$[i].x": 1}}),
    )

    # Concurrent $inc calls must each observe a distinct atomic post-image.
    c.insert({"_id": "counter", "n": 0})
    results: list[int] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            local = NonoDB(sock, timeout=60).collection(COLL)
            doc = local.find_and_modify(
                {"_id": "counter"}, {"$inc": {"n": 1}}, new=True
            )
            with lock:
                results.append(doc["n"])
        except BaseException as exc:  # noqa: BLE001 - surfaced below
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    assert sorted(results) == list(range(1, 21)), results
    assert c.find_one({"_id": "counter"})["n"] == 20

    print("find-and-modify OK")


if __name__ == "__main__":
    main()
