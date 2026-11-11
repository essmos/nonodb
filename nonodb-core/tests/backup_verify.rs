use std::fs::OpenOptions;
use std::io::Write;

use nonodb_core::heed::types::Bytes;
use nonodb_core::heed::{CompactionOption, Database, EnvOpenOptions};
use nonodb_core::{verify_backup, verify_data_dir, BackupManifest, HistoryConfig, Store, MAX_DBS};
use serde_json::{json, Value};
use tempfile::tempdir;

fn fixture() -> (tempfile::TempDir, tempfile::TempDir) {
    let data = tempdir().unwrap();
    let backup = tempdir().unwrap();
    let store = Store::open_with_history_and_map_size(
        data.path(),
        HistoryConfig {
            enabled: true,
            max_versions: 5,
        },
        16 * 1024 * 1024,
    )
    .unwrap();
    let _ = store.collection("users").unwrap();
    store.set_collection_ttl("users", Some(3600)).unwrap();
    let users = store.collection("users").unwrap();
    users
        .insert_one(
            json!({"_id": "u1", "grp": "a", "age": 30, "bio": "Rust developer", "embedding": [1.0, 0.0]}),
        )
        .unwrap();
    users
        .insert_one(
            json!({"_id": "u2", "grp": "a", "age": 42, "bio": "Go developer", "embedding": [0.0, 1.0]}),
        )
        .unwrap();
    users
        .insert_one(
            json!({"_id": "u3", "grp": "b", "age": 25, "bio": "Rust and SQL", "embedding": [0.9, 0.1]}),
        )
        .unwrap();
    users
        .create_index_fields(&["grp".into(), "age".into()], false, &[])
        .unwrap();
    users.create_index("age", false).unwrap();
    users.create_text_index("bio").unwrap();
    users.create_fuzzy_index("bio").unwrap();
    users
        .create_vector_index("embedding", 2, nonodb_core::VectorMetric::Cosine)
        .unwrap();
    users
        .update(&json!({"_id": "u1"}), &json!({"$inc": {"age": 1}}), false)
        .unwrap();

    let out = backup.path().join("data.mdb");
    store
        .copy_to_file(&out, CompactionOption::Enabled)
        .unwrap()
        .sync_all()
        .unwrap();
    let manifest = BackupManifest::from_backup(backup.path(), data.path()).unwrap();
    nonodb_core::verify::write_manifest(backup.path(), &manifest).unwrap();
    let closing = store.env().clone().prepare_for_closing();
    drop(users);
    drop(store);
    closing.wait();
    (data, backup)
}

#[test]
fn backup_manifest_and_deep_verification_succeed() {
    let (data, backup) = fixture();
    let report = verify_data_dir(data.path()).unwrap();
    assert_eq!(report.schema_version, 1);
    assert_eq!(report.documents, 3);
    assert_eq!(report.collections["users"].indexes.len(), 2);
    assert_eq!(report.collections["users"].text_indexes, vec!["bio"]);
    assert_eq!(report.collections["users"].fuzzy_indexes, vec!["bio"]);
    assert_eq!(
        report.collections["users"].vector_indexes,
        vec!["embedding"]
    );
    assert_eq!(report.vector_indexes, 1);
    assert!(report.history_entries >= 4);
    assert!(report.ttl_entries >= 3);

    let checked = verify_backup(backup.path()).unwrap();
    assert_eq!(checked.report.documents, report.documents);
    assert_eq!(checked.report.bytes, report.bytes);
    assert_eq!(
        checked.report.secondary_index_entries,
        report.secondary_index_entries
    );
    assert_eq!(
        checked.manifest.sha256,
        nonodb_core::verify::sha256_file(backup.path().join("data.mdb")).unwrap()
    );
}

fn rewrite_doc(data: &std::path::Path, id: &str, mutate: impl FnOnce(&mut Value)) {
    let env = unsafe { EnvOpenOptions::new().max_dbs(MAX_DBS).open(data).unwrap() };
    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.open_database(&wtxn, Some("users")).unwrap().unwrap();
    let raw = db.get(&wtxn, id.as_bytes()).unwrap().unwrap();
    let mut doc: Value = serde_json::from_slice(raw).unwrap();
    mutate(&mut doc);
    db.put(&mut wtxn, id.as_bytes(), &serde_json::to_vec(&doc).unwrap())
        .unwrap();
    wtxn.commit().unwrap();
    let closing = env.clone().prepare_for_closing();
    drop(env);
    closing.wait();
}

fn put_raw_doc(data: &std::path::Path, id: &str, raw: &[u8]) {
    let env = unsafe { EnvOpenOptions::new().max_dbs(MAX_DBS).open(data).unwrap() };
    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.open_database(&wtxn, Some("users")).unwrap().unwrap();
    db.put(&mut wtxn, id.as_bytes(), raw).unwrap();
    wtxn.commit().unwrap();
    let closing = env.clone().prepare_for_closing();
    drop(env);
    closing.wait();
}

#[test]
fn stale_document_index_entry_is_detected() {
    let (data, _backup) = fixture();
    rewrite_doc(data.path(), "u1", |doc| doc["age"] = json!(99));

    let err = verify_data_dir(data.path()).unwrap_err().to_string();
    assert!(
        err.contains("stale entry") || err.contains("missing entry") || err.contains("expected"),
        "{err}"
    );
}

#[test]
fn stale_text_and_fuzzy_entries_are_detected() {
    let (data, _backup) = fixture();
    rewrite_doc(data.path(), "u1", |doc| {
        doc["bio"] = json!("Completely different")
    });

    let err = verify_data_dir(data.path()).unwrap_err().to_string();
    assert!(
        err.contains("text index") || err.contains("fuzzy index"),
        "{err}"
    );
}

#[test]
fn invalid_json_document_is_detected() {
    let (data, _backup) = fixture();
    put_raw_doc(data.path(), "bad", b"{not-json");

    let err = verify_data_dir(data.path()).unwrap_err().to_string();
    assert!(err.contains("invalid JSON"), "{err}");
}

#[test]
fn manifest_checksum_and_size_are_enforced() {
    let (_data, backup) = fixture();
    let path = backup.path().join("data.mdb");
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(b"x").unwrap();
    drop(file);

    let err = verify_backup(backup.path()).unwrap_err().to_string();
    assert!(err.contains("size mismatch"), "{err}");
}

#[test]
fn manifest_hash_mismatch_is_detected() {
    let (_data, backup) = fixture();
    let manifest_path = backup.path().join("manifest.json");
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest["sha256"] = json!("00".repeat(32));
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let err = verify_backup(backup.path()).unwrap_err().to_string();
    assert!(err.contains("checksum mismatch"), "{err}");
}
