"""E2E test: index/search sub-database slot reuse.

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/db_slots_test.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB  # noqa: E402

COLL = "db_slots_e2e"


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
    c = db.collection(COLL)
    c.delete({}, multi=True)
    c.insert({"_id": "a", "kind": "x", "title": "hello world", "body": "hello fuzzy"})

    # More than the old 256-sub-database ceiling. Dropped slots must be reused.
    for i in range(270):
        field = f"f{i}"
        c.create_index(field)
        assert c.drop_index(field) is True

    for i in range(270):
        field = f"t{i}"
        c.create_text_index(field)
        assert c.drop_text_index(field) is True

    for i in range(270):
        field = f"z{i}"
        c.create_fuzzy_index(field)
        assert c.drop_fuzzy_index(field) is True

    c.create_index("kind")
    assert [d["_id"] for d in c.find({"kind": "x"})] == ["a"]

    c.create_text_index("title")
    assert [d["_id"] for d in c.search("title", "hello")] == ["a"]

    c.create_fuzzy_index("body")
    assert [d["_id"] for d in c.fuzzy("body", "helo")] == ["a"]

    # Reusing a slot after drop must not leak stale entries.
    assert c.drop_index("kind") is True
    c.update({"_id": "a"}, {"$set": {"kind": "y"}})
    c.create_index("kind")
    assert c.find({"kind": "x"}) == []
    assert [d["_id"] for d in c.find({"kind": "y"})] == ["a"]

    assert c.drop_text_index("title") is True
    c.update({"_id": "a"}, {"$set": {"title": "new text"}})
    c.create_text_index("title")
    assert c.search("title", "hello") == []
    assert [d["_id"] for d in c.search("title", "new")] == ["a"]

    print("db slots OK")


if __name__ == "__main__":
    main()
