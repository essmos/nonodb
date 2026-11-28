//! Regression coverage for standard/columnar parity gaps found in review.

use nonodb_core::{Collection, FindAndModifyOptions, FindQuery, Store};
use serde_json::{json, Value};

fn query(filter: Value, hint: Option<Value>) -> FindQuery {
    FindQuery {
        filter,
        projection: None,
        sort: Vec::new(),
        skip: 0,
        limit: None,
        comment: None,
        max_time_ms: None,
        hint,
    }
}

fn columnar<'a>(store: &'a Store, name: &'a str) -> Collection<'a> {
    store
        .create_columnar_collection(name, &nonodb_core::columnar::Settings::default())
        .unwrap()
}

#[test]
fn single_update_keeps_untouched_matches_indexed_and_unhistoried() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    columnar(&store, "docs");
    store.set_collection_history("docs", 10).unwrap();
    let coll = store.collection("docs").unwrap();
    coll.create_index("u", true).unwrap();
    for (id, u) in [("a", "a"), ("b", "b")] {
        coll.insert_one(json!({"_id": id, "group": "x", "u": u}))
            .unwrap();
    }

    assert_eq!(
        coll.update(
            &json!({"group": "x"}),
            &json!({"$set": {"hit": true}}),
            false,
        )
        .unwrap(),
        1
    );

    let survivors = coll.find(&query(json!({}), None)).unwrap();
    assert_eq!(survivors.len(), 2);
    for doc in survivors {
        let id = doc["_id"].as_str().unwrap();
        let u = doc["u"].as_str().unwrap();
        assert_eq!(
            coll.lookup("u", &[json!(u)], None).unwrap(),
            vec![Some(doc.clone())],
            "index entry for untouched document {id}"
        );
        let ops: Vec<_> = coll
            .history(id, None, false)
            .unwrap()
            .into_iter()
            .map(|entry| entry.op)
            .collect();
        if doc.get("hit").is_some() {
            assert_eq!(ops, vec!["update", "insert"]);
        } else {
            assert_eq!(ops, vec!["insert"], "untouched document {id} history");
        }
    }
}

#[test]
fn single_delete_keeps_untouched_matches_indexed_and_unhistoried() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    columnar(&store, "docs");
    store.set_collection_history("docs", 10).unwrap();
    let coll = store.collection("docs").unwrap();
    coll.create_index("u", true).unwrap();
    for (id, u) in [("a", "a"), ("b", "b")] {
        coll.insert_one(json!({"_id": id, "group": "x", "u": u}))
            .unwrap();
    }

    assert_eq!(coll.delete(&json!({"group": "x"}), false).unwrap(), 1);
    let survivors = coll.find(&query(json!({}), None)).unwrap();
    assert_eq!(survivors.len(), 1);
    let doc = &survivors[0];
    let id = doc["_id"].as_str().unwrap();
    let u = doc["u"].as_str().unwrap();
    assert_eq!(
        coll.lookup("u", &[json!(u)], None).unwrap(),
        vec![Some(doc.clone())],
        "surviving document index entry"
    );
    let ops: Vec<_> = coll
        .history(id, None, false)
        .unwrap()
        .into_iter()
        .map(|entry| entry.op)
        .collect();
    assert_eq!(ops, vec!["insert"], "surviving document history");
}

#[test]
fn find_and_modify_accepts_object_id() {
    let std_dir = tempfile::tempdir().unwrap();
    let col_dir = tempfile::tempdir().unwrap();
    let std_store = Store::open(std_dir.path()).unwrap();
    let col_store = Store::open(col_dir.path()).unwrap();
    let std_coll = std_store
        .create_collection("docs", Default::default())
        .unwrap();
    let col_coll = columnar(&col_store, "docs");
    let oid = "0123456789abcdef01234567";
    let id = json!({"$oid": oid});
    let update = json!({"$set": {"v": 2}});
    let filter = json!({"_id": {"$oid": oid}});
    std_coll.insert_one(json!({"_id": id, "v": 1})).unwrap();
    col_coll.insert_one(json!({"_id": id, "v": 1})).unwrap();

    let options = FindAndModifyOptions {
        filter: &filter,
        update: Some(&update),
        sort: &[],
        new: true,
        upsert: false,
        remove: false,
        array_filters: &[],
        projection: None,
    };
    assert_eq!(
        std_coll.find_and_modify(&options).unwrap().value,
        col_coll.find_and_modify(&options).unwrap().value
    );
    assert_eq!(col_coll.count(&json!({})).unwrap(), 1);

    let remove = FindAndModifyOptions {
        filter: &filter,
        update: None,
        sort: &[],
        new: false,
        upsert: false,
        remove: true,
        array_filters: &[],
        projection: None,
    };
    assert_eq!(
        std_coll.find_and_modify(&remove).unwrap().value,
        col_coll.find_and_modify(&remove).unwrap().value
    );
    assert_eq!(col_coll.count(&json!({})).unwrap(), 0);
}

#[test]
fn hints_and_explain_match_standard_index_contract() {
    let std_dir = tempfile::tempdir().unwrap();
    let col_dir = tempfile::tempdir().unwrap();
    let std_store = Store::open(std_dir.path()).unwrap();
    let col_store = Store::open(col_dir.path()).unwrap();
    let std_coll = std_store
        .create_collection("docs", Default::default())
        .unwrap();
    let col_coll = columnar(&col_store, "docs");
    for (id, u) in [("a", "a"), ("b", "b")] {
        std_coll.insert_one(json!({"_id": id, "u": u})).unwrap();
        col_coll.insert_one(json!({"_id": id, "u": u})).unwrap();
    }
    std_coll.create_index("u", false).unwrap();
    col_coll.create_index("u", false).unwrap();

    let hinted = query(json!({"u": "a"}), Some(json!({"u": 1})));
    assert_eq!(
        std_coll.find(&hinted).unwrap(),
        col_coll.find(&hinted).unwrap()
    );
    let std_explain = std_coll.explain(&hinted).unwrap();
    let col_explain = col_coll.explain(&hinted).unwrap();
    assert_eq!(col_explain["index_used"], json!(true));
    assert_eq!(col_explain["access_path"], std_explain["access_path"]);
    assert_eq!(
        col_explain["index"]["fields"],
        std_explain["index"]["fields"]
    );

    let invalid = query(json!({}), Some(json!({"missing": 1})));
    assert!(std_coll.find(&invalid).is_err());
    assert!(col_coll.find(&invalid).is_err());
    assert!(std_coll.explain(&invalid).is_err());
    assert!(col_coll.explain(&invalid).is_err());
}

#[test]
fn list_collection_info_reports_profile_and_settings() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store
        .create_collection("plain", Default::default())
        .unwrap();
    store
        .create_columnar_collection(
            "events",
            &nonodb_core::columnar::Settings {
                bucket_max_docs: 1024,
                ..Default::default()
            },
        )
        .unwrap();
    let info = store.collections_info().unwrap();
    let plain = info
        .iter()
        .find(|item| item["name"] == json!("plain"))
        .unwrap();
    assert_eq!(plain["profile"], json!("standard"));
    let events = info
        .iter()
        .find(|item| item["name"] == json!("events"))
        .unwrap();
    assert_eq!(events["profile"], json!("columnar"));
    assert_eq!(events["columnar"]["bucket_max_docs"], json!(1024));
}
