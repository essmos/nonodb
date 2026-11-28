//! Rollback/export parity test: columnar visible state to a standard collection.

use nonodb_core::{CollectionProfile, FindQuery, Store};
use serde_json::{json, Value};

fn all(coll: &nonodb_core::Collection<'_>) -> Vec<Value> {
    coll.find(&FindQuery {
        sort: vec![("_id".into(), 1)],
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn export_to_standard_preserves_documents_ids_indexes_ttl_and_history() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source_store = Store::open(source_dir.path()).unwrap();
    let target_store = Store::open(target_dir.path()).unwrap();
    source_store
        .create_columnar_collection("docs", &nonodb_core::columnar::Settings::default())
        .unwrap();
    target_store
        .create_collection("docs", CollectionProfile::Standard)
        .unwrap();
    source_store.set_collection_history("docs", 20).unwrap();
    target_store.set_collection_history("docs", 20).unwrap();
    source_store
        .set_collection_ttl("docs", Some(315_360_000))
        .unwrap();
    target_store
        .set_collection_ttl("docs", Some(315_360_000))
        .unwrap();
    let source = source_store.collection("docs").unwrap();
    let target = target_store.collection("docs").unwrap();
    source.create_index("v", true).unwrap();
    target.create_index("v", true).unwrap();
    source.create_index("tags", false).unwrap();
    target.create_index("tags", false).unwrap();

    source
        .insert_one(json!({
            "_id": "a",
            "v": 1,
            "tags": ["red", "blue"],
            "nested": {"n": 1, "ok": true},
            "when": {"$date": "2030-01-02T03:04:05Z"},
            "dec": {"$numberDecimal": "1.50"},
        }))
        .unwrap();
    source
        .insert_one(json!({"_id": "b", "v": 2, "tags": ["blue"], "nested": {"n": 2}}))
        .unwrap();
    source
        .insert_one(json!({"_id": "c", "v": 3, "tags": [], "nested": {"n": 3}}))
        .unwrap();
    source
        .update(
            &json!({"_id": "a"}),
            &json!({"$set": {"nested.n": 10, "tags": ["red"]}}),
            false,
        )
        .unwrap();
    source.delete(&json!({"_id": "c"}), false).unwrap();

    // Replay exported history oldest-first into the standard collection.
    for id in ["a", "b", "c"] {
        let mut history = source.history(id, None, true).unwrap();
        history.reverse();
        for entry in history {
            match entry.op.as_str() {
                "insert" => {
                    target.insert_one(entry.doc.unwrap()).unwrap();
                }
                "update" => {
                    let doc = entry.doc.unwrap();
                    let mut set = doc.as_object().unwrap().clone();
                    set.remove("_id");
                    target
                        .update(
                            &json!({"_id": id}),
                            &json!({"$set": Value::Object(set)}),
                            false,
                        )
                        .unwrap();
                }
                "delete" => {
                    target.delete(&json!({"_id": id}), false).unwrap();
                }
                other => panic!("unexpected history op {other}"),
            }
        }
    }

    let expected = all(&source);
    assert_eq!(all(&target), expected);
    assert_eq!(
        source.find_by_key("a").unwrap(),
        target.find_by_key("a").unwrap()
    );
    assert!(target.find_by_key("c").unwrap().is_none());
    assert_eq!(
        source.list_indexes().unwrap(),
        target.list_indexes().unwrap()
    );
    assert_eq!(
        source_store.collection_ttl("docs").unwrap(),
        target_store.collection_ttl("docs").unwrap()
    );
    assert_eq!(source.ttl_default_seconds(), target.ttl_default_seconds());
    for id in ["a", "b", "c"] {
        let source_history: Vec<_> = source
            .history(id, None, true)
            .unwrap()
            .into_iter()
            .map(|entry| (entry.op, entry.doc, entry.deleted))
            .collect();
        let target_history: Vec<_> = target
            .history(id, None, true)
            .unwrap()
            .into_iter()
            .map(|entry| (entry.op, entry.doc, entry.deleted))
            .collect();
        assert_eq!(source_history, target_history, "history mismatch for {id}");
    }
}
