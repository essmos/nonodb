use nonodb_core::{ttl, CollectionProfile, FindQuery, Store, VaultKey};
use serde_json::json;
use std::time::Duration;
use tempfile::TempDir;

fn store() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
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
    store
        .create_collection_with_ttl("events", CollectionProfile::Standard, Some(1))
        .unwrap();
    store.set_collection_history("events", 10).unwrap();
    let c = store.collection("events").unwrap();
    c.insert_one(json!({"_id": "gone", "n": 1})).unwrap();
    let due = store.ttl_due(ttl::now_ms() + 2_000, 10).unwrap();
    let entry = due.iter().find(|e| e.id == "gone").unwrap();

    let mut wtxn = store.write_txn().unwrap();
    let c = store.collection_in(&mut wtxn, "events").unwrap();
    let old = c
        .expire_txn_with_doc(&mut wtxn, "gone", entry.expire_at)
        .unwrap()
        .expect("expired document is returned for delete events");
    assert_eq!(old["n"], 1);
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
    store
        .create_collection_with_ttl_and_purge(
            "events",
            CollectionProfile::Standard,
            Some(1),
            Some(true),
        )
        .unwrap();
    store.set_collection_history("events", 10).unwrap();
    let c = store.collection("events").unwrap();
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
fn date_field_ttl_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let expires_at = ttl::now_ms() as i64 + 60_000;
    {
        let store = Store::open(dir.path()).unwrap();
        store
            .create_collection("events", CollectionProfile::Standard)
            .unwrap();
        store
            .set_collection_ttl_after("events", "expires_at", 30, None)
            .unwrap();
        let c = store.collection("events").unwrap();
        c.insert_one(json!({
            "_id": "future",
            "expires_at": {"$date": expires_at}
        }))
        .unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(
        store.collection_ttl_after("events").unwrap(),
        Some(("expires_at".to_string(), 30))
    );
    let c = store.collection("events").unwrap();
    let due = store.ttl_due(ttl::now_ms() + 120_000, 10).unwrap();
    assert!(due.iter().any(|entry| entry.id == "future"));
    assert!(c.find_one(&json!({"_id": "future"})).unwrap().is_some());
}

#[test]
fn backfill_sees_documents_hidden_by_previous_ttl() {
    let (_dir, store) = store();
    let c = store
        .create_collection("events", CollectionProfile::Standard)
        .unwrap();
    let future = ttl::now_ms() as i64 + 60_000;
    c.insert_one(json!({
        "_id": "hidden",
        "expires_at": {"$date": future},
        "_ttl": 1
    }))
    .unwrap();
    std::thread::sleep(Duration::from_millis(1_100));
    assert!(c.find_one(&json!({"_id": "hidden"})).unwrap().is_none());

    store
        .set_collection_ttl_after("events", "expires_at", 30, None)
        .unwrap();
    let mut wtxn = store.write_txn().unwrap();
    let page = store
        .backfill_collection_ttl_in(&mut wtxn, "events", "expires_at", 30, None, 100, false)
        .unwrap();
    wtxn.commit().unwrap();
    assert_eq!(page.updated, 1);
    let c = store.collection("events").unwrap();
    assert!(c.find_one(&json!({"_id": "hidden"})).unwrap().is_some());
    let due = store.ttl_due(ttl::now_ms() + 120_000, 10).unwrap();
    let entry = due.iter().find(|entry| entry.id == "hidden").unwrap();
    assert_eq!(entry.expire_at, future as u64 + 30_000);
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

#[test]
fn date_field_ttl_insert_update_and_backfill() {
    let (_dir, store) = store();
    let c = store
        .create_collection("events", CollectionProfile::Standard)
        .unwrap();
    let now = ttl::now_ms() as i64;
    c.insert_one(json!({
        "_id": "past",
        "expires_at": {"$date": now - 60_000}
    }))
    .unwrap();
    c.insert_one(json!({
        "_id": "future",
        "expires_at": {"$date": now + 60_000}
    }))
    .unwrap();
    c.insert_one(json!({"_id": "missing"})).unwrap();
    c.insert_one(json!({"_id": "not-a-date", "expires_at": "tomorrow"}))
        .unwrap();
    c.insert_one(json!({
        "_id": "explicit",
        "expires_at": {"$date": now + 60_000},
        "_ttl": 1
    }))
    .unwrap();

    store
        .set_collection_ttl_after("events", "expires_at", 30, None)
        .unwrap();
    assert_eq!(
        store.collection_ttl_after("events").unwrap(),
        Some(("expires_at".to_string(), 30))
    );
    let c = store.collection("events").unwrap();

    let mut after = None;
    loop {
        let mut wtxn = store.write_txn().unwrap();
        let page = store
            .backfill_collection_ttl_in(
                &mut wtxn,
                "events",
                "expires_at",
                30,
                after.as_ref(),
                2,
                false,
            )
            .unwrap();
        wtxn.commit().unwrap();
        after = page.next_after;
        if page.done {
            break;
        }
    }

    let due = store.ttl_due(ttl::now_ms() + 120_000, 100).unwrap();
    let ids: Vec<&str> = due.iter().map(|e| e.id.as_str()).collect();
    assert!(ids.contains(&"past"));
    assert!(ids.contains(&"future"));
    assert!(ids.contains(&"explicit"));
    assert!(!ids.contains(&"missing"));
    assert!(!ids.contains(&"not-a-date"));

    c.insert_one(json!({
        "_id": "inserted-later",
        "expires_at": {"$date": now + 90_000}
    }))
    .unwrap();
    c.update(
        &json!({"_id": "missing"}),
        &json!({"$set": {"expires_at": {"$date": now + 90_000}}}),
        false,
    )
    .unwrap();
    let due = store.ttl_due(ttl::now_ms() + 120_000, 100).unwrap();
    let ids: Vec<&str> = due.iter().map(|e| e.id.as_str()).collect();
    assert!(ids.contains(&"inserted-later"));
    assert!(ids.contains(&"missing"));

    c.update(
        &json!({"_id": "missing"}),
        &json!({"$set": {"expires_at": "tomorrow"}}),
        false,
    )
    .unwrap();
    let due = store.ttl_due(ttl::now_ms() + 120_000, 100).unwrap();
    assert!(!due.iter().any(|entry| entry.id == "missing"));
}

#[test]
fn date_field_ttl_backfill_paginates_large_collection() {
    let (_dir, store) = store();
    let c = store
        .create_collection("events", CollectionProfile::Standard)
        .unwrap();
    let future = ttl::now_ms() as i64 + 60_000;
    let mut wtxn = store.write_txn().unwrap();
    for i in 0..4_057 {
        c.insert_one_txn(
            &mut wtxn,
            json!({
                "_id": format!("{i:05}"),
                "expires_at": {"$date": future}
            }),
        )
        .unwrap();
    }
    wtxn.commit().unwrap();
    store
        .set_collection_ttl_after("events", "expires_at", 30, None)
        .unwrap();

    let mut after = None;
    let mut scanned = 0;
    let mut pages = 0;
    loop {
        let mut wtxn = store.write_txn().unwrap();
        let page = store
            .backfill_collection_ttl_in(
                &mut wtxn,
                "events",
                "expires_at",
                30,
                after.as_ref(),
                1_000,
                false,
            )
            .unwrap();
        wtxn.commit().unwrap();
        pages += 1;
        scanned += page.scanned;
        if page.done {
            assert_eq!(page.scanned, 57);
            break;
        }
        after = page.next_after;
        assert!(after.is_some());
    }
    assert_eq!(pages, 5);
    assert_eq!(scanned, 4_057);
    assert_eq!(
        store
            .ttl_due(ttl::now_ms() + 120_000, 10_000)
            .unwrap()
            .len(),
        4_057
    );
}

#[test]
fn date_field_ttl_dry_run_does_not_write_metadata() {
    let (_dir, store) = store();
    let c = store
        .create_collection("events", CollectionProfile::Standard)
        .unwrap();
    c.insert_one(json!({
        "_id": "a",
        "expires_at": {"$date": ttl::now_ms() as i64 + 60_000}
    }))
    .unwrap();
    let mut wtxn = store.write_txn().unwrap();
    let page = store
        .backfill_collection_ttl_in(&mut wtxn, "events", "expires_at", 30, None, 100, true)
        .unwrap();
    wtxn.commit().unwrap();
    assert_eq!(page.scanned, 1);
    assert_eq!(page.updated, 1);
    assert_eq!(page.cleared, 0);
    assert!(store
        .ttl_due(ttl::now_ms() + 120_000, 100)
        .unwrap()
        .is_empty());
}
