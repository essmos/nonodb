//! Uncommitted columnar transactions and interrupted maintenance are atomic.

use nonodb_core::{CollectionProfile, FindQuery, Store};
use serde_json::{json, Value};

fn snapshot(store: &Store, name: &str) -> Vec<Value> {
    let coll = store.collection(name).unwrap();
    coll.find(&FindQuery {
        filter: json!({}),
        projection: None,
        sort: vec![("_id".into(), 1)],
        skip: 0,
        limit: None,
        comment: None,
        max_time_ms: None,
        hint: None,
    })
    .unwrap()
}

fn history(store: &Store, id: &str) -> Vec<(String, Option<Value>, bool)> {
    let coll = store.collection("docs").unwrap();
    coll.history(id, None, true)
        .unwrap()
        .into_iter()
        .map(|entry| (entry.op, entry.doc, entry.deleted))
        .collect()
}

#[test]
fn dropped_columnar_transaction_rolls_back_data_index_ttl_history_and_settings() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store
        .create_columnar_collection(
            "docs",
            &nonodb_core::columnar::Settings {
                bucket_max_docs: 3,
                compression: nonodb_core::columnar::Settings::default().compression,
                bucket_max_bytes: 512 * 1024,
            },
        )
        .unwrap();
    store.set_collection_history("docs", 20).unwrap();
    store.set_collection_ttl("docs", Some(315_360_000)).unwrap();
    {
        let coll = store.collection("docs").unwrap();
        for index in 0..5 {
            coll.insert_one(json!({"_id": format!("d{index}"), "value": index}))
                .unwrap();
        }
        coll.create_index("value", false).unwrap();
        coll.update(
            &json!({"_id": "d0"}),
            &json!({"$set": {"value": 100}}),
            false,
        )
        .unwrap();
    }
    let before_docs = snapshot(&store, "docs");
    let before_indexes = store.collection("docs").unwrap().list_indexes().unwrap();
    let before_history = history(&store, "d0");
    let before_ttl = store.collection_ttl("docs").unwrap();
    let before_settings = store.columnar_settings("docs").unwrap();
    let before_stats = store.columnar_stats("docs").unwrap();

    {
        let mut wtxn = store.write_txn().unwrap();
        let coll = store.collection_in(&mut wtxn, "docs").unwrap();
        coll.insert_one_txn(&mut wtxn, json!({"_id": "uncommitted", "value": 7}))
            .unwrap();
        coll.update_txn(
            &mut wtxn,
            &json!({"_id": "d1"}),
            &json!({"$set": {"value": 999}}),
            false,
        )
        .unwrap();
        coll.delete_txn(&mut wtxn, &json!({"_id": "d2"}), false)
            .unwrap();
        coll.create_index_txn(&mut wtxn, "uncommitted-index", false)
            .unwrap();
        store
            .set_collection_ttl_in(&mut wtxn, "docs", Some(60))
            .unwrap();
        coll.set_columnar_settings_in(
            &mut wtxn,
            &nonodb_core::columnar::Settings {
                bucket_max_docs: 4,
                ..Default::default()
            },
        )
        .unwrap();
        drop(wtxn);
    }

    assert_eq!(snapshot(&store, "docs"), before_docs);
    assert_eq!(
        store.collection("docs").unwrap().list_indexes().unwrap(),
        before_indexes
    );
    assert_eq!(history(&store, "d0"), before_history);
    assert_eq!(store.collection_ttl("docs").unwrap(), before_ttl);
    assert_eq!(store.columnar_settings("docs").unwrap(), before_settings);
    assert_eq!(store.columnar_stats("docs").unwrap(), before_stats);

    // An interrupted compaction keeps all visible rows and the original keys.
    {
        let mut wtxn = store.write_txn().unwrap();
        let settings = store.columnar_settings("docs").unwrap().unwrap();
        nonodb_core::columnar::compact(&mut wtxn, store.env(), "docs", &settings).unwrap();
        drop(wtxn);
    }
    assert_eq!(snapshot(&store, "docs"), before_docs);

    // A dropped drop-collection transaction leaves the profile and sub-db intact.
    {
        let mut wtxn = store.write_txn().unwrap();
        assert!(store.drop_collection_in(&mut wtxn, "docs").unwrap());
        drop(wtxn);
    }
    assert_eq!(
        store.collection_profile("docs").unwrap(),
        Some(CollectionProfile::Columnar)
    );
    assert_eq!(snapshot(&store, "docs"), before_docs);

    drop(store);
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(snapshot(&store, "docs"), before_docs);
    assert_eq!(
        store.columnar_stats("docs").unwrap().unwrap().rows,
        before_docs.len() as u64
    );
}
