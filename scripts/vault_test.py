"""E2E test: optional vault collections (whole-document encryption).

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/vault_test.py

History is optional; enable it per collection to exercise it.
"""

from __future__ import annotations

import base64
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
    coll = f"vault_test_{os.getpid()}"
    db.create_collection(coll, "vault")

    key = base64.b64encode(os.urandom(32)).decode()
    other = base64.b64encode(os.urandom(32)).decode()
    vault = db.vault("acme", "acme:v1", key)
    docs = vault.collection(coll)

    doc_id = docs.insert(
        {
            "tenant_id": "acme",
            "secret": "s3cret",
            "payload": {"ssn": "123-45-6789"},
        }
    )
    got = docs.find_one({"_id": doc_id})
    assert got["secret"] == "s3cret", got
    assert got["payload"]["ssn"] == "123-45-6789", got
    assert docs.find({"tenant_id": "acme"})[0]["_id"] == doc_id
    assert docs.count({}) == 1

    # Wrong key and wrong tenant fail closed.
    try:
        db.vault("acme", "acme:v1", other).collection(coll).find_one(
            {"_id": doc_id}
        )
        raise AssertionError("wrong key was accepted")
    except NonoDBError as exc:
        assert exc.code == "decryption_failed", exc

    # Another tenant cannot see the document; lookup by _id yields null.
    assert (
        db.vault("other", "acme:v1", key).collection(coll).find_one(
            {"_id": doc_id}
        )
        is None
    )

    # Encrypted collections reject query features.
    for call in (
        lambda: docs.search("secret", "s3cret"),
        lambda: docs.fuzzy("secret", "s3cret"),
        lambda: docs.aggregate([{"$count": "n"}]),
        lambda: docs.find({"secret": "s3cret"}),
        lambda: docs.update({}, {"$set": {"secret": "x"}}, multi=True),
    ):
        try:
            call()
            raise AssertionError("vault operation unexpectedly succeeded")
        except NonoDBError as exc:
            assert exc.code in {
                "unsupported_encrypted_operation",
                "invalid_vault_filter",
            }, exc

    # Update and delete by _id.
    assert docs.update({"_id": doc_id}, {"$set": {"secret": "changed"}}) == 1
    assert docs.find_one({"_id": doc_id})["secret"] == "changed"
    assert docs.delete({"_id": doc_id}) == 1
    assert docs.count({}) == 0

    # Standard collections are untouched.
    standard = db.collection(f"standard_test_{os.getpid()}")
    sid = standard.insert({"name": "Ada", "age": 36})
    assert standard.find_one({"_id": sid})["name"] == "Ada"

    print("OK: vault encryption, tenant isolation, restrictions, CRUD")


if __name__ == "__main__":
    main()
