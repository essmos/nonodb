"""E2E test: online LMDB map growth.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/resize_test.py
"""

from __future__ import annotations

import os
import sys
import threading

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

COLL = "resize_e2e"


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
    c = db.collection(COLL)
    c.delete({}, multi=True)

    info = db.map_info()
    assert info["map_size"] >= info["used_bytes"] > 0
    assert info["page_size"] == 4096

    errors: list[BaseException] = []

    def worker(n: int) -> None:
        try:
            client = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
            coll = client.collection(COLL)
            for i in range(20):
                coll.insert({"worker": n, "i": i})
                coll.find({"worker": n})
            client.close()
        except BaseException as exc:  # noqa: BLE001 - propagate to main thread
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(4)]
    for thread in threads:
        thread.start()

    page = info["page_size"]
    target = info["map_size"]
    for _ in range(4):
        target += page * 16
        result = db.resize_map(target)
        assert result["map_size"] == target
        assert result["old_size"] < result["map_size"]

    for thread in threads:
        thread.join()
    if errors:
        raise errors[0]

    assert c.count({}) == 80
    assert c.find({"worker": 3}).__len__() == 20
    assert db.map_info()["map_size"] == target

    try:
        db.resize_map(target - page)
    except NonoDBError as exc:
        assert exc.code == "invalid_query", exc
    else:
        raise AssertionError("shrinking the map online must be rejected")

    print("resize OK")


if __name__ == "__main__":
    main()
