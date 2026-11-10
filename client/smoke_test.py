"""Smoke and concurrency test for nonodb (requires a running daemon).

Run with:
    NONODB_SOCK=/tmp/nonodb-smoke/nonodb.sock python3 client/smoke_test.py

Concurrency test: 12 threads (simulating 12 uvicorn workers) perform
`$inc` on the same document. The expected result is exact - this verifies
atomic read-modify-write and group commit.
"""

from __future__ import annotations

import os
import sys
import threading

sys.path.insert(0, os.path.dirname(__file__))
from nonodb import NonoDB, NonoDBError  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")
WORKERS = int(os.environ.get("WORKERS", "12"))
ITERS = int(os.environ.get("ITERS", "100"))


def main() -> int:
    db = NonoDB(SOCK)
    coll = db.collection("concurrency")
    coll.delete({}, multi=True)

    doc_id = coll.insert({"name": "counter", "n": 0})
    errors: list[BaseException] = []

    def worker() -> None:
        # Each thread has its own connection - like a separate uvicorn worker.
        local = NonoDB(SOCK)
        c = local.collection("concurrency")
        try:
            for _ in range(ITERS):
                c.update({"_id": doc_id}, {"$inc": {"n": 1}}, multi=False)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            local.close()

    threads = [threading.Thread(target=worker) for _ in range(WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        print(f"FAIL: {len(errors)} błędów, pierwszy: {errors[0]!r}")
        return 1

    got = coll.find_one({"_id": doc_id})["n"]
    expected = WORKERS * ITERS
    print(f"workers={WORKERS} iters={ITERS} expected={expected} got={got}")
    if got != expected:
        print("FAIL: zgubione aktualizacje (lost update)")
        return 1

    # A transport error must be NonoDBError("connection"), not raw httpx.
    ghost = NonoDB("/tmp/nonodb-nie-istnieje.sock", timeout=0.2)
    try:
        ghost.collection("x").count({})
    except NonoDBError as exc:
        if exc.code != "connection":
            print(f"FAIL: oczekiwano connection, jest {exc.code}")
            return 1
    else:
        print("FAIL: brak błędu przy nieistniejącym gnieździe")
        return 1
    try:
        bad_health = ghost.health()
    finally:
        ghost.close()
    if bad_health:
        print("FAIL: health() powinien być False")
        return 1

    print("OK: atomowy read-modify-write + group commit działa")
    print("OK: błędy transportu -> NonoDBError(connection)")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
