use nonodb_core::{FindQuery, Store};
use serde_json::json;
use tempfile::TempDir;

fn collection() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("explain").unwrap();
    for doc in [
        json!({"_id": "a", "a": 1, "b": 10}),
        json!({"_id": "b", "a": 2, "b": 20}),
        json!({"_id": "c", "a": 3, "b": 30}),
    ] {
        c.insert_one(doc).unwrap();
    }
    c.create_index("a", false).unwrap();
    (dir, store)
}

fn query(filter: serde_json::Value) -> FindQuery {
    FindQuery {
        filter,
        projection: None,
        sort: Vec::new(),
        skip: 0,
        limit: None,
    }
}

#[test]
fn explain_reports_index_bounds_and_counts() {
    let (_dir, store) = collection();
    let c = store.collection("explain").unwrap();
    let mut q = query(json!({"a": {"$gte": 2}}));
    q.sort = vec![("a".to_string(), 1)];
    q.limit = Some(5);
    let e = c.explain(&q).unwrap();
    assert_eq!(e["access_path"], "index_scan");
    assert_eq!(e["index_used"], true);
    assert_eq!(e["index"]["fields"], json!(["a"]));
    assert_eq!(e["candidates"], 2);
    assert_eq!(e["actual_matched"], 2);
    assert_eq!(e["actual_returned"], 2);
    assert_eq!(e["sort_covered"], true);
    assert!(e["bounds"]["lower"]["value"].is_string());
}

#[test]
fn explain_reports_id_lookup_and_id_range() {
    let (_dir, store) = collection();
    let c = store.collection("explain").unwrap();

    let e = c.explain(&query(json!({"_id": "b"}))).unwrap();
    assert_eq!(e["access_path"], "id_lookup");
    assert_eq!(e["candidates"], 1);
    assert_eq!(e["actual_matched"], 1);
    assert_eq!(e["actual_returned"], 1);

    let e = c
        .explain(&query(json!({"_id": {"$gte": "a", "$lt": "c"}})))
        .unwrap();
    assert_eq!(e["access_path"], "id_range");
    assert_eq!(e["candidates"], 2);
    assert_eq!(e["actual_matched"], 2);
    assert_eq!(e["bounds"]["lower"]["value"], "a");
    assert_eq!(e["bounds"]["upper"]["value"], "c");
    assert_eq!(e["bounds"]["upper"]["inclusive"], false);
}

#[test]
fn explain_reports_scan_skip_and_limit() {
    let (_dir, store) = collection();
    let c = store.collection("explain").unwrap();
    let mut q = query(json!({"b": {"$gt": 15}}));
    q.sort = vec![("a".to_string(), 1)];
    q.skip = 1;
    q.limit = Some(1);
    let e = c.explain(&q).unwrap();
    assert_eq!(e["access_path"], "collection_scan");
    assert_eq!(e["index_used"], false);
    assert_eq!(e["candidates"], 3);
    assert_eq!(e["actual_matched"], 2);
    assert_eq!(e["actual_returned"], 1);
    assert_eq!(e["sort_covered"], false);
    assert_eq!(e["skip"], 1);
    assert_eq!(e["limit"], 1);
}

#[test]
fn explain_rejects_invalid_filters() {
    let (_dir, store) = collection();
    let c = store.collection("explain").unwrap();
    assert!(c.explain(&query(json!({"a": {"$regex": "["}}))).is_err());
    assert!(c.explain(&query(json!({"a": {"$all": 1}}))).is_err());
}
