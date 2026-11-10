"""Regression tests for optional vault collections.

Run against a daemon:

    NONODB_SOCK=/tmp/nonodb.sock python3 scripts/vault_regression_test.py

Set NONODB_HISTORY=1 on the daemon to exercise vault history as well.
"""

from __future__ import annotations

import base64
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

TENANT = "reg-tenant"
OTHER_TENANT = "reg-other"
KID = "reg:v1"
# Deterministic test key only. Never use a hard-coded key in production.
KEY = base64.b64encode(bytes([7] * 32)).decode()
WRONG_KEY = base64.b64encode(bytes([8] * 32)).decode()
VAULT = "vault_regression"
STANDARD = "vault_regression_std"


def expect(code: str, fn):
    try:
        fn()
    except NonoDBError as exc:
        assert exc.code == code, f"expected {code}, got {exc.code}: {exc}"
        return exc
    raise AssertionError(f"expected error {code}, operation succeeded")


def reset(db: NonoDB, vault) -> None:
    db.create_collection(VAULT, "vault")
    db.create_collection(STANDARD, "standard")
    db.collection(STANDARD).delete({}, multi=True)
    vc = vault.collection(VAULT)
    while True:
        docs = vc.find({"tenant_id": TENANT}, limit=1000)
        if not docs:
            break
        for doc in docs:
            vc.delete({"_id": doc["_id"]})


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=120)
    vault = db.vault(TENANT, KID, KEY)
    other = db.vault(TENANT, KID, WRONG_KEY)
    oc = other.collection(VAULT)
    other_tenant = db.vault(OTHER_TENANT, KID, KEY)
    otc = other_tenant.collection(VAULT)
    vc = vault.collection(VAULT)
    std = db.collection(STANDARD)
    headers = {
        "x-nonodb-tenant": TENANT,
        "x-nonodb-key-id": KID,
        "x-nonodb-key": KEY,
    }

    reset(db, vault)

    # ----- profiles -------------------------------------------------
    db.create_collection(VAULT, "vault")  # idempotent same profile
    expect("vault_profile_immutable", lambda: db.create_collection(VAULT, "standard"))
    expect("vault_profile_immutable", lambda: db.create_collection(STANDARD, "vault"))
    expect(
        "invalid_collection_profile",
        lambda: db.create_collection("vault_bad_profile", "wat"),
    )

    # ----- required headers / wrong collection kind ------------------
    expect("vault_key_required", lambda: db.collection(VAULT).find_one({"_id": "missing"}))
    expect("vault_key_required", lambda: db.collection(VAULT).count({}))
    expect("vault_key_unexpected", lambda: vault.collection(STANDARD).find_one({}))
    expect(
        "not_a_vault_collection",
        lambda: vault.collection("vault_does_not_exist").find_one({}),
    )
    assert "vault_does_not_exist" not in db.collections()

    # All three headers are required together.
    expect(
        "vault_key_required",
        lambda: db.exec(
            {"op": "find", "coll": VAULT, "filter": {}},
            headers={"x-nonodb-tenant": TENANT},
        ),
    )
    expect(
        "invalid_vault_key",
        lambda: db.exec(
            {"op": "find", "coll": VAULT, "filter": {}},
            headers={"x-nonodb-tenant": TENANT, "x-nonodb-key-id": KID, "x-nonodb-key": "%%%"},
        ),
    )
    expect(
        "invalid_vault_key",
        lambda: db.exec(
            {"op": "find", "coll": VAULT, "filter": {}},
            headers={
                "x-nonodb-tenant": TENANT,
                "x-nonodb-key-id": KID,
                "x-nonodb-key": base64.b64encode(b"short").decode(),
            },
        ),
    )

    # ----- insert / tenant injection --------------------------------
    id1 = vc.insert({"secret": "one"})  # tenant_id injected from header
    assert vc.find_one({"_id": id1})["tenant_id"] == TENANT
    id2 = vc.insert({"tenant_id": TENANT, "secret": "two", "n": 1})
    expect(
        "tenant_mismatch",
        lambda: vc.insert({"tenant_id": OTHER_TENANT, "secret": "bad"}),
    )
    expect("invalid_vault_document", lambda: vc.insert({"tenant_id": 123}))
    expect("duplicate_key", lambda: vc.insert({"_id": id1, "secret": "dup"}))

    ids = vc.insert_many([{"secret": "many-1"}, {"secret": "many-2"}])
    assert len(ids) == 2
    assert all(vc.find_one({"_id": i}) for i in ids)

    # ----- read isolation and restricted filters --------------------
    assert vc.find_one({"_id": id1})["secret"] == "one"
    assert len(vc.find({"tenant_id": TENANT})) == 4
    assert vc.count({"tenant_id": TENANT}) == 4
    assert vc.count({}) == 4
    assert otc.find_one({"_id": id1}) is None
    expect("tenant_mismatch", lambda: vc.find({"tenant_id": OTHER_TENANT}))
    expect("invalid_vault_filter", lambda: vc.find({"secret": "one"}))
    expect("invalid_vault_filter", lambda: vc.find({"n": {"$gte": 0}}))
    expect("invalid_vault_filter", lambda: vc.count({"secret": "one"}))
    expect(
        "unsupported_encrypted_operation",
        lambda: vc.find({}, projection={"secret": 1}),
    )
    expect(
        "unsupported_encrypted_operation",
        lambda: vc.find({}, sort=[["secret", 1]]),
    )
    expect(
        "decryption_failed",
        lambda: oc.find_one({"_id": id1}),
    )
    expect(
        "decryption_failed",
        lambda: db.vault(TENANT, "reg:v2", KEY).collection(VAULT).find_one({"_id": id1}),
    )

    asc = vc.find({"tenant_id": TENANT}, sort=[["_id", 1]])
    desc = vc.find({"tenant_id": TENANT}, sort=[["_id", -1]])
    assert [d["_id"] for d in asc] == list(reversed([d["_id"] for d in desc]))
    page = vc.find({"tenant_id": TENANT}, sort=[["_id", 1]], skip=1, limit=2)
    assert [d["_id"] for d in page] == [d["_id"] for d in asc[1:3]]

    # ----- unsupported query features --------------------------------
    for op in (
        lambda: vc.search("secret", "one"),
        lambda: vc.fuzzy("secret", "one"),
        lambda: vc.aggregate([{"$count": "n"}]),
        lambda: vc.create_index("secret"),
        lambda: vc.create_text_index("secret"),
        lambda: vc.create_fuzzy_index("secret"),
        lambda: vc.list_indexes(),
        lambda: vc.list_text_indexes(),
        lambda: vc.list_fuzzy_indexes(),
        lambda: db.exec({"op": "batch", "ops": []}, headers=headers),
        lambda: db.exec(
            {
                "op": "write_batch",
                "ops": [{"op": "insert", "coll": VAULT, "doc": {"secret": "x"}}],
            },
            headers=headers,
        ),
    ):
        expect("unsupported_encrypted_operation", op)

    # ----- update ----------------------------------------------------
    assert vc.update({"_id": id2}, {"$set": {"secret": "changed"}}) == 1
    assert vc.find_one({"_id": id2})["secret"] == "changed"
    assert vc.update({"_id": id2}, {"$inc": {"n": 2}}) == 1
    assert vc.find_one({"_id": id2})["n"] == 3
    assert vc.update({"_id": id2}, {"$push": {"tags": "x"}}) == 1
    assert vc.find_one({"_id": id2})["tags"] == ["x"]

    expect(
        "invalid_vault_filter",
        lambda: vc.update({"tenant_id": TENANT}, {"$set": {"n": 0}}, multi=True),
    )
    expect(
        "invalid_vault_filter",
        lambda: vc.update({"tenant_id": TENANT}, {"$set": {"n": 0}}),
    )
    expect("invalid_id", lambda: vc.update({"_id": id2}, {"$set": {"_id": "new"}}))
    expect(
        "tenant_mismatch",
        lambda: vc.update({"_id": id2}, {"$set": {"tenant_id": OTHER_TENANT}}),
    )
    expect(
        "decryption_failed",
        lambda: oc.update({"_id": id2}, {"$set": {"secret": "bad"}}),
    )
    expect(
        "tenant_mismatch",
        lambda: otc.update({"_id": id2}, {"$set": {"secret": "bad"}}),
    )
    assert vc.update({"_id": "missing"}, {"$set": {"x": 1}}) == 0

    upsert = vc.update_result(
        {"_id": "upsert-vault-id"},
        {"$set": {"secret": "upserted"}, "$setOnInsert": {"created": True}},
        upsert=True,
    )
    assert upsert["upserted_id"] == "upsert-vault-id", upsert
    assert vc.find_one({"_id": "upsert-vault-id"})["created"] is True

    # ----- history (optional) ---------------------------------------
    revisions = vc.history(id2, limit=20, include_docs=True)
    if revisions:
        assert revisions[0]["doc"]["secret"] == "changed", revisions
        assert revisions[0]["doc"]["tenant_id"] == TENANT, revisions
        rev = revisions[0]["rev"]
        assert vc.get_version(id2, rev)["doc"]["secret"] == "changed"
        expect(
            "decryption_failed",
            lambda: oc.get_version(id2, rev),
        )
    else:
        print("history disabled on daemon; vault history checks skipped")

    # ----- delete ----------------------------------------------------
    expect(
        "invalid_vault_filter",
        lambda: vc.delete({"tenant_id": TENANT}, multi=True),
    )
    expect("invalid_vault_filter", lambda: vc.delete({"tenant_id": TENANT}))
    expect(
        "decryption_failed",
        lambda: oc.delete({"_id": id2}),
    )
    expect(
        "tenant_mismatch",
        lambda: otc.delete({"_id": id2}),
    )
    assert vc.delete({"_id": "missing"}) == 0
    assert vc.delete({"_id": id2}) == 1
    assert vc.find_one({"_id": id2}) is None

    # ----- standard collection is unaffected ------------------------
    sid = std.insert({"name": "Ada", "age": 36})
    assert std.find_one({"_id": sid})["name"] == "Ada"
    assert std.count({}) == 1
    std.delete({"_id": sid})
    assert std.count({}) == 0

    # Avoid leaving test documents behind (collections stay registered).
    while True:
        docs = vc.find({"tenant_id": TENANT}, limit=1000)
        if not docs:
            break
        for doc in docs:
            vc.delete({"_id": doc["_id"]})

    print("OK: vault regression suite passed")
    db.close()


if __name__ == "__main__":
    main()
