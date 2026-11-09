"""Test cache odczytów klienta (wymaga działającego daemona).

    NONODB_SOCK=/tmp/nonodb.sock python3 client/cache_test.py
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from nonodb import NonoDB  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")


def main() -> int:
    cached = NonoDB(SOCK, cache_ttl=5.0)
    plain = NonoDB(SOCK)

    c = cached.collection("cachetest")
    p = plain.collection("cachetest")
    c.delete({}, multi=True)
    doc_id = c.insert({"v": 1})

    assert c.find_one({"_id": doc_id})["v"] == 1

    # Inny proces zmienia dokument — cache w `cached` trzyma starą wartość (TTL).
    p.update({"_id": doc_id}, {"$set": {"v": 2}})
    assert c.find_one({"_id": doc_id})["v"] == 1, "cache powinien zwrócić starą wartość"

    # Zapis przez tę samą instancję czyści cache.
    c.update({"_id": doc_id}, {"$set": {"v": 3}})
    assert c.find_one({"_id": doc_id})["v"] == 3

    # Zwracana wartość to kopia — mutacja nie psuje cache.
    got = c.find_one({"_id": doc_id})
    got["v"] = 999
    assert c.find_one({"_id": doc_id})["v"] == 3

    # TTL: po wygaśnięciu widać zmianę z innego procesu.
    short = NonoDB(SOCK, cache_ttl=0.2)
    s = short.collection("cachetest")
    assert s.find_one({"_id": doc_id})["v"] == 3
    p.update({"_id": doc_id}, {"$set": {"v": 4}})
    assert s.find_one({"_id": doc_id})["v"] == 3, "jeszcze w TTL"
    time.sleep(0.25)
    assert s.find_one({"_id": doc_id})["v"] == 4, "po TTL powinien odświeżyć"

    # Wyłączony cache (domyślnie) zawsze widzi świeże dane.
    assert p.find_one({"_id": doc_id})["v"] == 4

    cached.close()
    plain.close()
    short.close()
    print("OK: cache TTL + invalidacja przy zapisie + kopie wartości")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
