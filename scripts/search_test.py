"""Test end-to-end: full-text BM25 i fuzzy search przez daemona.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/search_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")


def main() -> int:
    db = NonoDB(SOCK)
    c = db.collection("kb")
    c.delete({}, multi=True)
    c.drop_text_index("body")
    c.drop_fuzzy_index("title")

    c.insert({"title": "PostgreSQL", "body": "relational database with sql transactions"})
    c.insert({"title": "MySQL", "body": "relational database popular for web applications"})
    c.insert({"title": "MongoDB", "body": "document database with json storage"})
    c.insert({"title": "Rust async", "body": "rust language with async runtime and memory safety"})
    c.insert({"title": "Python web", "body": "python framework for web applications"})

    c.create_text_index("body")
    c.create_fuzzy_index("title")
    assert c.list_text_indexes() == ["body"]
    assert c.list_fuzzy_indexes() == ["title"]

    # --- BM25 -------------------------------------------------------
    hits = c.search("body", "database")
    titles = {h["title"] for h in hits}
    assert titles == {"PostgreSQL", "MySQL", "MongoDB"}, titles
    scores = [h["_score"] for h in hits]
    assert scores == sorted(scores, reverse=True), scores

    # OR-semantyka: "relational" + "database" razem wygrywa z samym "database".
    hits = c.search("body", "relational database")
    assert {h["title"] for h in hits[:2]} == {"PostgreSQL", "MySQL"}, hits

    hits = c.search("body", "rust")
    assert [h["title"] for h in hits] == ["Rust async"]

    hits = c.search("body", "database", filter={"title": "MySQL"})
    assert [h["title"] for h in hits] == ["MySQL"]

    hits = c.search("body", "database", limit=2)
    assert len(hits) == 2

    # update: MongoDB przestaje pasować do "json"
    c.update({"title": "MongoDB"}, {"$set": {"body": "distributed document store"}})
    assert c.search("body", "json") == []
    assert len(c.search("body", "database")) == 2

    # --- fuzzy ------------------------------------------------------
    hits = c.fuzzy("title", "mangodb", max_distance=2)
    assert hits and hits[0]["title"] == "MongoDB", hits
    assert hits[0]["_distance"] <= 2

    hits = c.fuzzy("title", "postgress", max_distance=3)
    assert any(h["title"] == "PostgreSQL" for h in hits), hits

    hits = c.fuzzy("title", "rustt", max_distance=1)
    assert any(h["title"] == "Rust async" for h in hits), hits

    hits = c.fuzzy("title", "mysql", max_distance=0, filter={"title": "MySQL"})
    assert [h["title"] for h in hits] == ["MySQL"]

    assert c.fuzzy("title", "mysql", max_distance=0, filter={"title": "PostgreSQL"}) == []

    # delete usuwa z obu indeksów
    c.delete({"title": "MySQL"}, multi=False)
    assert c.fuzzy("title", "mysql", max_distance=0) == []
    assert {h["title"] for h in c.search("body", "database")} == {"PostgreSQL"}

    # --- sprzątanie -------------------------------------------------
    assert c.drop_text_index("body") is True
    assert c.drop_fuzzy_index("title") is True
    assert c.list_text_indexes() == []
    assert c.list_fuzzy_indexes() == []
    for fn in (lambda: c.search("body", "x"), lambda: c.fuzzy("title", "x")):
        try:
            fn()
            raise AssertionError("expected error after drop")
        except NonoDBError as e:
            assert e.code == "invalid_query", e.code

    print("OK: BM25 (ranking, filtr, update/delete) + fuzzy (_distance, max_distance, filtr)")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
