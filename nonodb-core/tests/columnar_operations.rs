//! Replication, backup, compaction, and accounting tests for columnar collections.

use nonodb_core::{FindQuery, Store};
use serde_json::json;

fn columnar<'a>(store: &'a Store, name: &'a str) -> nonodb_core::Collection<'a> {
    store
        .create_columnar_collection(name, &nonodb_core::columnar::Settings::default())
        .unwrap()
}

#[test]
fn columnar_replication_apply_is_idempotent_and_maintains_indexes() {
    let pd = tempfile::tempdir().unwrap();
    let rd = tempfile::tempdir().unwrap();
    let primary_store = Store::open(pd.path()).unwrap();
    let replica_store = Store::open(rd.path()).unwrap();
    let primary = columnar(&primary_store, "docs");
    let replica = columnar(&replica_store, "docs");
    replica.create_index("v", true).unwrap();

    let id = primary.insert_one(json!({"_id": "a", "v": 1})).unwrap();
    let after = primary.find_by_key(&id).unwrap().unwrap();
    let mut wtxn = replica_store.write_txn().unwrap();
    replica
        .apply_replication_change_txn(&mut wtxn, "insert", &id, Some(&after), None)
        .unwrap();
    wtxn.commit().unwrap();
    assert_eq!(
        replica.find_by_key("a").unwrap(),
        Some(json!({"_id": "a", "v": 1}))
    );
    assert_eq!(
        replica.lookup("v", &[json!(1)], None).unwrap(),
        vec![Some(json!({"_id": "a", "v": 1}))]
    );

    let mut wtxn = replica_store.write_txn().unwrap();
    assert!(replica
        .apply_replication_change_txn(&mut wtxn, "insert", &id, Some(&after), None)
        .is_err());
    drop(wtxn);

    let updated = json!({"_id": "a", "v": 2});
    let mut wtxn = replica_store.write_txn().unwrap();
    replica
        .apply_replication_change_txn(&mut wtxn, "update", &id, Some(&updated), None)
        .unwrap();
    wtxn.commit().unwrap();
    assert_eq!(replica.find_by_key("a").unwrap(), Some(updated));
    assert!(replica.lookup("v", &[json!(1)], None).unwrap()[0].is_none());
    assert_eq!(
        replica.lookup("v", &[json!(2)], None).unwrap()[0],
        Some(json!({"_id": "a", "v": 2}))
    );

    let mut wtxn = replica_store.write_txn().unwrap();
    replica
        .apply_replication_change_txn(&mut wtxn, "delete", &id, None, None)
        .unwrap();
    wtxn.commit().unwrap();
    assert!(replica.find_by_key("a").unwrap().is_none());
    assert!(replica.lookup("v", &[json!(2)], None).unwrap()[0].is_none());

    let oid = json!({"$oid": "0123456789abcdef01234567"});
    let object_id = primary.insert_one(json!({"_id": oid, "v": 3})).unwrap();
    let after = primary.find_by_key(&object_id).unwrap().unwrap();
    let mut wtxn = replica_store.write_txn().unwrap();
    replica
        .apply_replication_change_txn(&mut wtxn, "insert", &object_id, Some(&after), None)
        .unwrap();
    wtxn.commit().unwrap();
    assert_eq!(replica.find_by_key(&object_id).unwrap(), Some(after));
    let mut wtxn = replica_store.write_txn().unwrap();
    replica
        .apply_replication_change_txn(&mut wtxn, "delete", &object_id, None, None)
        .unwrap();
    wtxn.commit().unwrap();
    assert!(replica.find_by_key(&object_id).unwrap().is_none());
}

#[test]
fn columnar_backup_copy_and_reopen_preserves_documents() {
    let source_dir = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let source = Store::open(source_dir.path()).unwrap();
    let c = columnar(&source, "docs");
    for index in 0..20 {
        c.insert_one(json!({"_id": format!("d{index:02}"), "v": index}))
            .unwrap();
    }
    source.seal_columnar("docs").unwrap();
    let backup_path = backup_dir.path().join("data.mdb");
    source
        .copy_to_file(&backup_path, nonodb_core::heed::CompactionOption::Enabled)
        .unwrap();
    drop(source);

    let restored = Store::open(backup_dir.path()).unwrap();
    let r = restored.collection("docs").unwrap();
    assert_eq!(r.count(&json!({})).unwrap(), 20);
    assert_eq!(r.find_by_key("d07").unwrap().unwrap()["v"], 7);
    assert_eq!(
        r.find(&FindQuery {
            filter: json!({"v": {"$gte": 18}}),
            sort: vec![("v".into(), -1)],
            ..Default::default()
        })
        .unwrap()[0]["_id"],
        "d19"
    );
}

#[test]
fn columnar_compaction_merges_overlays_and_preserves_visible_state() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = columnar(&store, "docs");
    c.create_index("v", true).unwrap();
    for index in 0..30 {
        c.insert_one(json!({"_id": format!("d{index:02}"), "v": index}))
            .unwrap();
    }
    store.seal_columnar("docs").unwrap();
    for index in 0..10 {
        c.update(
            &json!({"_id": format!("d{index:02}")}),
            &json!({"$set": {"v": index + 100}}),
            false,
        )
        .unwrap();
    }
    for index in 20..30 {
        c.delete(&json!({"_id": format!("d{index:02}")}), false)
            .unwrap();
    }
    let before = c
        .find(&FindQuery {
            sort: vec![("_id".into(), 1)],
            ..Default::default()
        })
        .unwrap();
    let rows = store.compact_columnar("docs").unwrap();
    assert_eq!(rows, before.len());
    let after = c
        .find(&FindQuery {
            sort: vec![("_id".into(), 1)],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(c.find_by_key("d00").unwrap().unwrap()["v"], 100);
    assert!(c.find_by_key("d20").unwrap().is_none());
    assert_eq!(
        c.lookup("v", &[json!(100)], None).unwrap()[0],
        Some(json!({"_id": "d00", "v": 100}))
    );
}

#[test]
fn columnar_storage_and_database_stats_include_buckets_and_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = columnar(&store, "docs");
    let docs: Vec<_> = (0..50)
        .map(|index| json!({"_id": format!("d{index:02}"), "v": index}))
        .collect();
    let mut wtxn = store.write_txn().unwrap();
    c.insert_many_txn(&mut wtxn, &docs).unwrap();
    wtxn.commit().unwrap();
    c.create_index("v", false).unwrap();
    assert_eq!(c.delete(&json!({"v": {"$lt": 5}}), true).unwrap(), 5);
    // The statistic must count visible rows in row-oriented staging buckets
    // without decoding every document.
    let rtxn = store.read_txn().unwrap();
    let stats = c.storage_stats(&rtxn).unwrap();
    assert_eq!(stats.records, 45);
    assert!(stats.data_bytes > 0);
    assert!(stats.index_bytes > 0);
    assert_eq!(stats.indexes, 1);
    let raw = nonodb_core::columnar::stats(&rtxn, store.env(), "docs").unwrap();
    assert_eq!(raw.rows, 45);
    assert_eq!(raw.deletes, 5);
    assert!(raw.open_bytes > 0);
    assert!(raw.id_bytes > 0);
    rtxn.commit().unwrap();
    store.seal_columnar("docs").unwrap();
    let rtxn = store.read_txn().unwrap();
    assert_eq!(c.storage_stats(&rtxn).unwrap().records, 45);
    let raw = nonodb_core::columnar::stats(&rtxn, store.env(), "docs").unwrap();
    assert_eq!(raw.sealed_bytes, raw.data_bytes);
    rtxn.commit().unwrap();

    let db = store.storage_stats().unwrap();
    let collection = db
        .collections
        .iter()
        .find(|item| item.collection == "docs")
        .unwrap();
    assert_eq!(collection.records, 45);
    assert!(collection.data_bytes > 0);
    assert!(collection.index_bytes > 0);
}
