use nonodb_core::{ttl, CollectionProfile, FindQuery, HistoryConfig, Store, VaultKey};
use serde_json::json;
use tempfile::TempDir;

fn store() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open_with_history(
        dir.path(),
        HistoryConfig {
            enabled: true,
            max_versions: 10,
        },
    )
    .unwrap();
    (dir, store)
}

#[test]
fn collection_default_and_document_override() {
    let (_dir, store) = store();
    let c = store
        .create_collection_with_ttl("events", CollectionProfile::Standard, Some(60))
        .unwrap();
    assert_eq!(c.ttl_default_seconds(), Some(60));
    assert_eq!(store.collection_ttl("events").unwrap(), Some(60));

    let inherited = c
        .insert_one(json!({"_id": "inherited", "kind": "default"}))
        .unwrap();
    let disabled = c
        .insert_one(json!({"_id": "disabled", "kind": "never", "_ttl": 0}))
        .unwrap();
    let override_id = c
        .insert_one(json!({"_id": "override", "kind": "short", "_ttl": 1}))
        .unwrap();

    let now = ttl::now_ms();
    let due = store.ttl_due(now + 120_000, 100).unwrap();
    let ids: Vec<&str> = due.iter().map(|e| e.id.as_str()).collect();
    assert!(ids.contains(&inherited.as_str()));
    assert!(ids.contains(&override_id.as_str()));
    assert!(!ids.contains(&disabled.as_str()));

    let docs = c
        .find(&FindQuery {
            filter: json!({"_id": inherited}),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert!(docs[0].get("_ttl").is_none());
}

#[test]
fn update_preserves_or_refreshes_ttl() {
    let (_dir, store) = store();
    let c = store
        .create_collection_with_ttl("events", CollectionProfile::Standard, Some(60))
        .unwrap();
    let id = c.insert_one(json!({"_id": "a", "n": 1})).unwrap();
    let first_due = store.ttl_due(ttl::now_ms() + 120_000, 100).unwrap();
    let first = first_due.iter().find(|e| e.id == id).unwrap().expire_at;

    c.update(&json!({"_id": id}), &json!({"$set": {"n": 2}}), false)
        .unwrap();
    let after_update = store.ttl_due(ttl::now_ms() + 120_000, 100).unwrap();
    assert_eq!(
        after_update.iter().find(|e| e.id == id).unwrap().expire_at,
        first
    );

    c.update(&json!({"_id": id}), &json!({"$set": {"_ttl": 1}}), false)
        .unwrap();
    let after_refresh = store.ttl_due(ttl::now_ms() + 120_000, 100).unwrap();
    assert!(after_refresh.iter().any(|e| e.id == id));

    c.update(&json!({"_id": id}), &json!({"$set": {"_ttl": 0}}), false)
        .unwrap();
    let cleared = store.ttl_due(ttl::now_ms() + 120_000, 100).unwrap();
    assert!(!cleared.iter().any(|e| e.id == id));
}

#[test]
fn expiry_deletes_document_and_records_tombstone() {
    let (_dir, store) = store();
    let c = store
        .create_collection_with_ttl("events", CollectionProfile::Standard, Some(1))
        .unwrap();
    c.insert_one(json!({"_id": "gone", "n": 1})).unwrap();
    let due = store.ttl_due(ttl::now_ms() + 2_000, 10).unwrap();
    let entry = due.iter().find(|e| e.id == "gone").unwrap();

    let mut wtxn = store.write_txn().unwrap();
    let c = store.collection_in(&mut wtxn, "events").unwrap();
    assert!(c.expire_txn(&mut wtxn, "gone", entry.expire_at).unwrap());
    assert!(!c.expire_txn(&mut wtxn, "gone", entry.expire_at).unwrap());
    wtxn.commit().unwrap();

    assert!(c
        .find(&FindQuery {
            filter: json!({"_id": "gone"}),
            ..Default::default()
        })
        .unwrap()
        .is_empty());
    assert!(store.ttl_due(ttl::now_ms() + 2_000, 10).unwrap().is_empty());
    let history = c.history("gone", Some(10), false).unwrap();
    assert_eq!(history[0].op, "delete");
}

#[test]
fn stale_expiry_does_not_delete_refreshed_document() {
    let (_dir, store) = store();
    let c = store
        .create_collection_with_ttl("events", CollectionProfile::Standard, Some(60))
        .unwrap();
    c.insert_one(json!({"_id": "race", "n": 1})).unwrap();
    let due = store.ttl_due(ttl::now_ms() + 120_000, 10).unwrap();
    let old = due.iter().find(|e| e.id == "race").unwrap().expire_at;

    c.update(
        &json!({"_id": "race"}),
        &json!({"$set": {"_ttl": 1}}),
        false,
    )
    .unwrap();

    let mut wtxn = store.write_txn().unwrap();
    let c = store.collection_in(&mut wtxn, "events").unwrap();
    assert!(!c.expire_txn(&mut wtxn, "race", old).unwrap());
    wtxn.commit().unwrap();
    assert_eq!(c.count(&json!({"_id": "race"})).unwrap(), 1);
}

#[test]
fn expiry_can_purge_history() {
    let (_dir, store) = store();
    let c = store
        .create_collection_with_ttl_and_purge(
            "events",
            CollectionProfile::Standard,
            Some(1),
            Some(true),
        )
        .unwrap();
    assert!(c.ttl_purge_history());
    c.insert_one(json!({"_id": "gone", "n": 1})).unwrap();
    assert_eq!(c.history("gone", Some(10), false).unwrap().len(), 1);
    let due = store.ttl_due(ttl::now_ms() + 2_000, 10).unwrap();
    let entry = due.iter().find(|e| e.id == "gone").unwrap();

    let mut wtxn = store.write_txn().unwrap();
    let c = store.collection_in(&mut wtxn, "events").unwrap();
    assert!(c.expire_txn(&mut wtxn, "gone", entry.expire_at).unwrap());
    wtxn.commit().unwrap();
    assert!(c.history("gone", Some(10), false).unwrap().is_empty());
}

#[test]
fn vault_ttl_stores_plaintext_metadata() {
    let (_dir, store) = store();
    let key = VaultKey::from_bytes([21u8; 32]);
    let c = store
        .create_collection_with_ttl("secrets", CollectionProfile::Vault, Some(1))
        .unwrap();
    let mut wtxn = store.write_txn().unwrap();
    c.insert_vault_txn(
        &mut wtxn,
        json!({"tenant_id": "acme", "secret": "hunter2"}),
        &key,
        "acme",
        "acme:v1",
    )
    .unwrap();
    wtxn.commit().unwrap();

    let due = store.ttl_due(ttl::now_ms() + 2_000, 10).unwrap();
    assert_eq!(due.len(), 1);
    let mut wtxn = store.write_txn().unwrap();
    let c = store.collection_in(&mut wtxn, "secrets").unwrap();
    assert!(c
        .expire_txn(&mut wtxn, &due[0].id, due[0].expire_at)
        .unwrap());
    wtxn.commit().unwrap();
    assert!(store.ttl_due(ttl::now_ms() + 2_000, 10).unwrap().is_empty());
}

#[test]
fn expiry_removes_secondary_index_entries() {
    let (_dir, store) = store();
    let c = store
        .create_collection_with_ttl("events", CollectionProfile::Standard, Some(1))
        .unwrap();
    c.create_index("kind", false).unwrap();
    c.insert_one(json!({"_id": "indexed", "kind": "timer"}))
        .unwrap();
    assert_eq!(c.count(&json!({"kind": "timer"})).unwrap(), 1);

    let due = store.ttl_due(ttl::now_ms() + 2_000, 10).unwrap();
    let entry = due.iter().find(|e| e.id == "indexed").unwrap();
    let mut wtxn = store.write_txn().unwrap();
    let c = store.collection_in(&mut wtxn, "events").unwrap();
    assert!(c.expire_txn(&mut wtxn, "indexed", entry.expire_at).unwrap());
    wtxn.commit().unwrap();
    assert_eq!(c.count(&json!({"kind": "timer"})).unwrap(), 0);
}

#[test]
fn ttl_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        let c = store
            .create_collection_with_ttl("events", CollectionProfile::Standard, Some(1))
            .unwrap();
        c.insert_one(json!({"_id": "persisted"})).unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    let due = store.ttl_due(ttl::now_ms() + 2_000, 10).unwrap();
    assert!(due.iter().any(|e| e.id == "persisted"));
}

#[test]
fn invalid_ttl_is_rejected_without_storing() {
    let (_dir, store) = store();
    let c = store
        .create_collection_with_ttl("events", CollectionProfile::Standard, None)
        .unwrap();
    assert!(c.insert_one(json!({"_id": "bad", "_ttl": "soon"})).is_err());
    assert_eq!(c.count(&json!({})).unwrap(), 0);
    assert!(store
        .create_collection_with_ttl("bad", CollectionProfile::Standard, Some(0))
        .is_err());
}
