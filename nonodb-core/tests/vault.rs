use nonodb_core::heed::types::Bytes;
use nonodb_core::{CollectionProfile, FindQuery, HistoryConfig, Store, VaultKey};
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
fn vault_round_trip_and_isolation() {
    let (_dir, store) = store();
    let key = VaultKey::from_bytes([11u8; 32]);
    let other = VaultKey::from_bytes([12u8; 32]);
    let vault = store
        .create_collection("vault", CollectionProfile::Vault)
        .unwrap();
    assert_eq!(vault.profile(), CollectionProfile::Vault);
    let names = store.collections().unwrap();
    assert!(names.contains(&"vault".to_string()));
    assert!(!names.iter().any(|name| name.ends_with(":profile")));

    let mut wtxn = store.write_txn().unwrap();
    let id2 = vault
        .insert_vault_txn(
            &mut wtxn,
            json!({"tenant_id":"acme","secret":"other","n":2}),
            &key,
            "acme",
            "acme:v1",
        )
        .unwrap();
    wtxn.commit().unwrap();

    let rtxn = store.read_txn().unwrap();
    let raw_db = store
        .env()
        .open_database::<Bytes, Bytes>(&rtxn, Some("vault"))
        .unwrap()
        .unwrap();
    let raw = raw_db.get(&rtxn, id2.as_bytes()).unwrap().unwrap();
    let raw_text = std::str::from_utf8(raw).unwrap();
    assert!(raw_text.contains("_enc"));
    assert!(!raw_text.contains("other"));
    assert!(!raw_text.contains("s3cret"));

    let docs = vault
        .find_vault_in(
            &rtxn,
            &FindQuery {
                filter: json!({"tenant_id":"acme"}),
                sort: vec![("_id".into(), 1)],
                skip: 0,
                limit: None,
                projection: None,
            },
            &key,
            "acme",
            "acme:v1",
        )
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["secret"], "other");
    assert!(vault
        .find_vault_in(
            &rtxn,
            &FindQuery {
                filter: json!({"tenant_id":"acme"}),
                ..Default::default()
            },
            &other,
            "acme",
            "acme:v1",
        )
        .is_err());
    assert!(vault
        .find_vault_in(
            &rtxn,
            &FindQuery {
                filter: json!({"secret":"other"}),
                ..Default::default()
            },
            &key,
            "acme",
            "acme:v1",
        )
        .is_err());
    rtxn.commit().unwrap();

    let mut wtxn = store.write_txn().unwrap();
    let out = vault
        .update_vault_txn(
            &mut wtxn,
            &json!({"_id": id2}),
            &json!({"$set":{"secret":"changed"}}),
            false,
            false,
            &key,
            "acme",
            "acme:v1",
        )
        .unwrap();
    assert_eq!(out.matched, 1);
    let deleted = vault
        .delete_vault_txn(
            &mut wtxn,
            &json!({"_id": id2}),
            false,
            &key,
            "acme",
            "acme:v1",
        )
        .unwrap();
    assert_eq!(deleted, 1);
    wtxn.commit().unwrap();

    let rtxn = store.read_txn().unwrap();
    assert_eq!(vault.count_vault_in(&rtxn, &json!({}), "acme").unwrap(), 0);
    rtxn.commit().unwrap();

    // Standard write paths cannot bypass encryption.
    let mut wtxn = store.write_txn().unwrap();
    assert!(vault
        .insert_one_txn(&mut wtxn, json!({"tenant_id":"acme","secret":"plain"}))
        .is_err());
    assert!(store
        .create_collection_in(&mut wtxn, "vault", CollectionProfile::Standard)
        .is_err());
    wtxn.commit().unwrap();
}

#[test]
fn history_decrypts_snapshots() {
    let (_dir, store) = store();
    let key = VaultKey::from_bytes([13u8; 32]);
    let vault = store
        .create_collection("vault", CollectionProfile::Vault)
        .unwrap();
    let mut wtxn = store.write_txn().unwrap();
    let id = vault
        .insert_vault_txn(
            &mut wtxn,
            json!({"tenant_id":"acme","secret":"v1"}),
            &key,
            "acme",
            "acme:v1",
        )
        .unwrap();
    vault
        .update_vault_txn(
            &mut wtxn,
            &json!({"_id": id}),
            &json!({"$set":{"secret":"v2"}}),
            false,
            false,
            &key,
            "acme",
            "acme:v1",
        )
        .unwrap();
    wtxn.commit().unwrap();

    let rtxn = store.read_txn().unwrap();
    let entries = vault
        .history_vault_in(&rtxn, &id, None, true, &key, "acme", "acme:v1")
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].doc.as_ref().unwrap()["secret"], "v2");
    assert_eq!(entries[1].doc.as_ref().unwrap()["secret"], "v1");
    rtxn.commit().unwrap();
}
