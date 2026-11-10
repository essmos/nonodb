"""Extended search tests: BM25, fuzzy, and query operators.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/search_variants_test.py

Each section clears its collections, so the test can be run repeatedly.
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

SOCK = os.environ.get("NONODB_SOCK", "/run/nonodb.sock")


def ids(hits: list[dict]) -> list[str]:
    return [h["_id"] for h in hits]


def clear(c) -> None:
    c.delete({}, multi=True)
    for f in c.list_text_indexes():
        c.drop_text_index(f)
    for f in c.list_fuzzy_indexes():
        c.drop_fuzzy_index(f)


def expect_error(fn, code: str, label: str) -> None:
    try:
        fn()
    except NonoDBError as e:
        assert e.code == code, f"{label}: {e.code} != {code}"
    else:
        raise AssertionError(f"{label}: brak błędu {code}")


def test_bm25(db: NonoDB) -> None:
    c = db.collection("sv_text")
    clear(c)
    docs = [
        {"_id": "a", "title": "Alpha", "body": "rust rust rust async runtime", "cat": "a"},
        {"_id": "b", "title": "Beta", "body": "rust async", "cat": "a"},
        {"_id": "c", "title": "Gamma", "body": "python web framework", "cat": "b"},
        {"_id": "d", "title": "Delta", "body": "database database sql", "cat": "b"},
        {"_id": "e", "title": "Epsilon", "body": "database sql", "cat": "a"},
        {"_id": "f", "title": "Zeta", "body": "ZAŻÓŁĆ GĘŚLĄ JAŹŃ", "cat": "c"},
        {"_id": "g", "title": "Eta", "body": "state-of-the-art rust 2024", "cat": "c"},
        {"_id": "h", "title": "Theta", "body": "", "cat": "c"},
        {"_id": "i", "title": "Iota", "body": "postgresql database", "cat": "d"},
    ]
    for d in docs:
        c.insert(d)
    c.create_index("cat")
    c.create_text_index("body")

    # Single term + sorting by _score descending.
    hits = c.search("body", "rust", limit=10)
    assert set(ids(hits)) == {"a", "b", "g"}, hits
    assert hits[0]["_id"] == "a"  # 3x rust beats single occurrences
    assert all(h["_score"] > 0 for h in hits)
    assert [h["_score"] for h in hits] == sorted((h["_score"] for h in hits), reverse=True)

    # OR: "async" narrows the top results, but g still matches through "rust" alone.
    # The shorter document with both terms (b) beats the long one (a).
    hits = c.search("body", "rust async", limit=10)
    assert set(ids(hits)) == {"a", "b", "g"}, hits
    assert hits[0]["_id"] == "b"

    # Duplicates in the query do not change the result.
    assert ids(c.search("body", "rust rust rust")) == ids(c.search("body", "rust"))

    # Case and punctuation.
    assert set(ids(c.search("body", "RUST"))) == {"a", "b", "g"}
    assert ids(c.search("body", "state-of-the-art")) == ["g"]
    assert ids(c.search("body", "state of the art")) == ["g"]
    assert ids(c.search("body", "2024")) == ["g"]

    # Unicode: lowercase + diacritic folding.
    assert ids(c.search("body", "zażółć")) == ["f"]
    assert ids(c.search("body", "ZAŻÓŁĆ")) == ["f"]
    assert ids(c.search("body", "zazolc")) == ["f"]

    # Phrases require a contiguous token sequence.
    c.insert({"_id": "phrase", "body": "Ala ma kota", "cat": "x"})
    assert ids(c.search("body", '"ala ma"')) == ["phrase"]
    assert c.search("body", '"ma ala"') == []

    # IDF: a rarer term has more weight than a common one.
    rare = c.search("body", "postgresql")[0]["_score"]
    common = c.search("body", "database")[0]["_score"]
    assert rare > common, (rare, common)

    # Filter, skip, limit.
    hits = c.search("body", "rust", filter={"cat": "a"})
    assert set(ids(hits)) == {"a", "b"}
    hits = c.search("body", "database", filter={"cat": "b"})
    assert set(ids(hits)) == {"d"}  # e ma cat=a, i ma cat=d
    hits = c.search("body", "database", skip=1, limit=1)
    assert len(hits) == 1
    assert ids(hits) != ids(c.search("body", "database", limit=1))
    assert c.search("body", "database", limit=0) == []
    assert c.search("body", "database", skip=99) == []

    # No hits / empty query / punctuation only.
    assert c.search("body", "nieistniejetermin") == []
    assert c.search("body", "") == []
    assert c.search("body", " ... ,, !! ") == []
    assert c.search("body", "database", filter={"cat": "nie-ma"}) == []

    # Update removes old terms and adds new ones.
    c.update({"_id": "a"}, {"$set": {"body": "kot pies"}})
    assert set(ids(c.search("body", "rust"))) == {"b", "g"}
    assert ids(c.search("body", "kot")) == ["a"]
    c.update({"_id": "a"}, {"$set": {"body": "rust"}})
    assert set(ids(c.search("body", "rust"))) == {"a", "b", "g"}

    # Delete clears postings and lengths.
    c.delete({"_id": "d"})
    assert set(ids(c.search("body", "database"))) == {"e", "i"}

    # A second text index on another field works independently.
    c.create_text_index("title")
    assert set(ids(c.search("title", "rust"))) == set()
    assert ids(c.search("title", "eta")) == ["g"]
    assert set(ids(c.search("body", "rust"))) == {"a", "b", "g"}
    c.drop_text_index("title")
    expect_error(lambda: c.search("title", "eta"), "invalid_query", "text po dropie")

    # Drop + recreate (backfill) works.
    c.drop_text_index("body")
    expect_error(lambda: c.search("body", "rust"), "invalid_query", "text przed create")
    c.create_text_index("body")
    assert set(ids(c.search("body", "rust"))) == {"a", "b", "g"}

    # Empty query after deleting all documents.
    c.delete({}, multi=True)
    assert c.search("body", "rust") == []

    print("OK: BM25 — tf, OR, IDF, filtr, skip/limit, Unicode, update/delete, reindex")


def test_bm25_typed_fields(db: NonoDB) -> None:
    c = db.collection("sv_types")
    clear(c)

    # Numbers and bools are indexed as text.
    c.insert({"_id": "n1", "code": 42, "flag": True})
    c.insert({"_id": "n2", "code": 7, "flag": False})
    c.create_text_index("code")
    c.create_text_index("flag")
    assert ids(c.search("code", "42")) == ["n1"]
    assert set(ids(c.search("code", "7"))) == {"n2"}
    assert ids(c.search("flag", "true")) == ["n1"]
    assert ids(c.search("flag", "false")) == ["n2"]

    # Arrays/objects are skipped by the text index (without error).
    a = db.collection("sv_arrays")
    clear(a)
    a.insert({"_id": "arr1", "body": ["rust", "async"], "meta": {"x": 1}})
    a.create_text_index("body")
    a.create_text_index("meta")
    assert a.search("body", "rust") == []
    assert a.search("meta", "x") == []

    print("OK: BM25 — liczby/bool indeksowane, tablice/obiekty pomijane")


def test_fuzzy(db: NonoDB) -> None:
    c = db.collection("sv_fuzzy")
    clear(c)
    docs = [
        {"_id": "t1", "title": "PostgreSQL", "cat": "db"},
        {"_id": "t2", "title": "MySQL", "cat": "db"},
        {"_id": "t3", "title": "MongoDB", "cat": "db"},
        {"_id": "t4", "title": "Rust async runtime", "cat": "lang"},
        {"_id": "t5", "title": "Python web framework", "cat": "lang"},
        {"_id": "t6", "title": "Zażółć gęślą jaźń", "cat": "pl"},
        {"_id": "t7", "title": "state-of-the-art", "cat": "lang"},
        {"_id": "t8", "title": "database", "cat": "db"},
    ]
    for d in docs:
        c.insert(d)
    c.create_index("cat")
    c.create_fuzzy_index("title")

    # Distance 0 (exact match, including case).
    hits = c.fuzzy("title", "mysql", max_distance=0)
    assert ids(hits) == ["t2"] and hits[0]["_distance"] == 0
    assert ids(c.fuzzy("title", "POSTGRESQL", max_distance=0)) == ["t1"]

    # Typos and transpositions.
    hits = c.fuzzy("title", "mangodb", max_distance=1)
    assert ids(hits) == ["t3"] and hits[0]["_distance"] == 1
    assert c.fuzzy("title", "mangodb", max_distance=0) == []
    hits = c.fuzzy("title", "postgresqi", max_distance=1)
    assert "t1" in ids(hits) and hits[0]["_distance"] == 1
    hits = c.fuzzy("title", "pythno", max_distance=2)
    assert "t5" in ids(hits)
    assert c.fuzzy("title", "pythno", max_distance=1) == []

    # Sorting by _distance ascending.
    hits = c.fuzzy("title", "databse", max_distance=2)
    assert hits and hits[0]["_id"] == "t8" and hits[0]["_distance"] == 1
    assert [h["_distance"] for h in hits] == sorted(h["_distance"] for h in hits)

    # Unicode / diacritics: folding also works in fuzzy search.
    assert ids(c.fuzzy("title", "Zażółć", max_distance=0)) == ["t6"]
    assert ids(c.fuzzy("title", "zażółć gęślą jaźń", max_distance=0)) == ["t6"]
    assert ids(c.fuzzy("title", "zazolc", max_distance=1)) == ["t6"]

    # Punctuation and tokens.
    assert ids(c.fuzzy("title", "state-of-the-art", max_distance=0)) == ["t7"]
    assert ids(c.fuzzy("title", "runtime", max_distance=0)) == ["t4"]

    # Filter and limit.
    assert ids(c.fuzzy("title", "mysql", max_distance=0, filter={"cat": "db"})) == ["t2"]
    assert c.fuzzy("title", "mysql", max_distance=0, filter={"cat": "lang"}) == []
    many = c.fuzzy("title", "a", max_distance=2, limit=2)
    assert len(many) <= 2

    # No hits / empty query / limit 0.
    assert c.fuzzy("title", "qqqqqq", max_distance=1) == []
    assert c.fuzzy("title", "", max_distance=1) == []
    assert c.fuzzy("title", "database", limit=0) == []

    # Update and delete keep the trigram index in sync.
    c.update({"_id": "t2"}, {"$set": {"title": "MariaDB"}})
    assert c.fuzzy("title", "mysql", max_distance=0) == []
    assert ids(c.fuzzy("title", "mariadb", max_distance=0)) == ["t2"]
    c.delete({"_id": "t3"})
    assert c.fuzzy("title", "mangodb", max_distance=2) == []

    # Dropped index -> error.
    c.drop_fuzzy_index("title")
    expect_error(lambda: c.fuzzy("title", "mysql"), "invalid_query", "fuzzy po dropie")

    print("OK: fuzzy — dystanse 0/1/2, transpozycje, ranking, filtr, Unicode, update/delete")


def test_query_operators(db: NonoDB) -> None:
    c = db.collection("sv_query")
    clear(c)
    c.insert({"_id": "q1", "a": 1, "tags": ["x", "y"], "nest": {"v": "z"}})
    c.insert({"_id": "q2", "a": 3, "tags": ["y"], "nest": {"v": "w"}})
    c.insert({"_id": "q3", "a": None})
    c.insert({"_id": "q4"})
    c.insert({"_id": "q5", "a": 5, "tags": []})
    c.create_index("a")

    assert set(ids(c.find({"a": 1}))) == {"q1"}
    assert set(ids(c.find({"a": {"$eq": 1}}))) == {"q1"}
    # `$ne`/`$nin` also match null and missing fields (MongoDB semantics).
    assert set(ids(c.find({"a": {"$ne": 1}}))) == {"q2", "q3", "q4", "q5"}
    assert set(ids(c.find({"a": {"$gt": 1, "$lt": 4}}))) == {"q2"}
    assert set(ids(c.find({"a": {"$gte": 1, "$lte": 3}}))) == {"q1", "q2"}
    assert set(ids(c.find({"a": {"$in": [1, 5]}}))) == {"q1", "q5"}
    assert set(ids(c.find({"a": {"$nin": [1, 5]}}))) == {"q2", "q3", "q4"}
    assert set(ids(c.find({"a": {"$exists": True}}))) == {"q1", "q2", "q3", "q5"}
    assert set(ids(c.find({"a": {"$exists": False}}))) == {"q4"}
    assert set(ids(c.find({"a": None}))) == {"q3", "q4"}

    assert set(ids(c.find({"$and": [{"a": {"$gte": 1}}, {"a": {"$lt": 5}}]}))) == {"q1", "q2"}
    assert set(ids(c.find({"$or": [{"a": 1}, {"a": 5}]}))) == {"q1", "q5"}
    assert set(ids(c.find({"$nor": [{"a": 1}, {"a": 5}]}))) == {"q2", "q3", "q4"}
    assert set(ids(c.find({"$not": {"a": {"$gte": 1}}}))) == {"q3", "q4"}

    # Nested field and array matching.
    assert set(ids(c.find({"nest.v": "z"}))) == {"q1"}
    c.create_index("nest.v")
    assert set(ids(c.find({"nest.v": "z"}))) == {"q1"}
    assert c.count({"nest.v": "z"}) == 1
    assert set(ids(c.find({"tags": "x"}))) == {"q1"}
    assert set(ids(c.find({"tags": {"$in": ["y"]}}))) == {"q1", "q2"}

    # Sort/projection/skip/limit.
    got = c.find({}, sort=[("a", 1)], projection={"a": 1, "_id": 1})
    # null and missing fields sort last (missing last).
    assert [d["_id"] for d in got] == ["q1", "q2", "q5", "q3", "q4"]
    assert set(got[2]) == {"_id", "a"}
    got = c.find({}, sort=[("a", -1)], skip=1, limit=2, projection={"a": 0})
    assert len(got) == 2 and all("a" not in d for d in got)
    assert len(c.find({}, skip=2, limit=2)) == 2

    assert c.count({}) == 5
    assert c.count({"a": {"$gte": 1}}) == 3
    assert c.count({"a": None}) == 2
    assert c.find_one({"a": 3})["_id"] == "q2"

    print("OK: operatory zapytań — eq/ne/range/in/nin/exists/and/or/nor/not, nested, sort, projekcja")


def main() -> int:
    db = NonoDB(SOCK)
    test_bm25(db)
    test_bm25_typed_fields(db)
    test_fuzzy(db)
    test_query_operators(db)

    for name in ("sv_text", "sv_types", "sv_arrays", "sv_fuzzy", "sv_query"):
        clear(db.collection(name))

    print("OK: wszystkie warianty wyszukiwania")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
