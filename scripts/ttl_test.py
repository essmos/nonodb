"""E2E test: document TTL, collection defaults, overrides, and the sweeper.

Run against a daemon with a short sweep interval, e.g.:

    NONODB_TTL_SWEEP_INTERVAL=1 NONODB_SOCK=/tmp/nonodb.sock \
      python3 scripts/ttl_test.py
"""

from __future__ import annotations

import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "client"))

from nonodb import NonoDB  # noqa: E402


def wait_for(fn, timeout: float = 12.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fn():
            return
        time.sleep(0.15)
    raise AssertionError("timed out waiting for TTL sweep")


def main() -> None:
    db = NonoDB(os.environ.get("NONODB_SOCK", "/run/nonodb.sock"), timeout=60)
    name = "ttl_e2e"
    c = db.collection(name)
    c.delete({}, multi=True)
    db.create_collection(name, ttl_seconds=1)
    assert db.get_collection_ttl(name) == 1

    inherited = c.insert({"_id": "inherited", "v": 1})
    disabled = c.insert({"_id": "disabled", "v": 2, "_ttl": 0})
    override = c.insert({"_id": "override", "v": 3, "_ttl": 30})
    assert "_ttl" not in c.find_one({"_id": inherited})
    assert c.find_one({"_id": inherited})["v"] == 1

    wait_for(lambda: c.find_one({"_id": inherited}) is None)
    assert c.find_one({"_id": disabled}) is not None
    assert c.find_one({"_id": override}) is not None

    # TTL is metadata: update without `_ttl` preserves it, explicit `_ttl` refreshes.
    c.update({"_id": disabled}, {"$set": {"_ttl": 1}})
    wait_for(lambda: c.find_one({"_id": disabled}) is None)
    c.update({"_id": override}, {"$set": {"_ttl": 0}})
    time.sleep(1.5)
    assert c.find_one({"_id": override}) is not None

    # Clearing the collection default stops applying it to new documents.
    db.set_collection_ttl(name, None)
    assert db.get_collection_ttl(name) is None
    no_ttl = c.insert({"_id": "no-default", "v": 4})
    time.sleep(1.5)
    assert c.find_one({"_id": no_ttl}) is not None

    # Setting a collection default later applies only to new inserts.
    db.set_collection_ttl(name, 1)
    later = c.insert({"_id": "later", "v": 5})
    wait_for(lambda: c.find_one({"_id": later}) is None)

    # TTL deletion records a history tombstone when history is enabled.
    hist = c.history(later)
    if hist:
        assert hist[0]["op"] == "delete", hist

    # Opt-in history purge on TTL deletion.
    purge_name = "ttl_purge_e2e"
    db.create_collection(
        purge_name, ttl_seconds=1, ttl_purge_history=True
    )
    pc = db.collection(purge_name)
    purge_id = pc.insert({"_id": "purged", "v": 1})
    wait_for(lambda: pc.find_one({"_id": purge_id}) is None)
    assert pc.history(purge_id) == []

    # Vault collections use the same TTL metadata without decrypting for expiry.
    vault_name = "ttl_vault_e2e"
    vdb = db.vault("acme", "acme:v1", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
    try:
        vdb.collection(vault_name).delete({"_id": "secret"}, multi=False)
    except Exception:
        pass
    db.create_collection(vault_name, profile="vault", ttl_seconds=1)
    vc = vdb.collection(vault_name)
    vc.insert({"_id": "secret", "tenant_id": "acme", "value": "hidden"})
    assert vc.find_one({"_id": "secret"})["value"] == "hidden"
    wait_for(lambda: vc.find_one({"_id": "secret"}) is None)

    print("OK: TTL defaults, overrides, update semantics, sweeper, history, vault")
    db.close()


if __name__ == "__main__":
    main()
