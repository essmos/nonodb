use nonodb_core::{Error, FindQuery, Store};
use serde_json::json;
use tempfile::TempDir;

fn store() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (dir, store)
}

fn find(c: &nonodb_core::Collection<'_>, filter: serde_json::Value) -> Vec<serde_json::Value> {
    c.find(&FindQuery {
        filter,
        ..Default::default()
    })
    .unwrap()
}

fn ids(docs: &[serde_json::Value]) -> Vec<&str> {
    docs.iter().filter_map(|d| d["_id"].as_str()).collect()
}

#[test]
fn all_size_and_elem_match() {
    let (_dir, store) = store();
    let c = store.collection("arrays").unwrap();
    c.insert_one(json!({
        "_id": "a",
        "tags": ["rust", "lmdb"],
        "nums": [1, 2, 3],
        "items": [
            {"sku": "x", "qty": 2},
            {"sku": "y", "qty": 9}
        ]
    }))
    .unwrap();
    c.insert_one(json!({
        "_id": "b",
        "tags": ["rust"],
        "nums": [5],
        "items": [{"sku": "x", "qty": 10}]
    }))
    .unwrap();
    c.insert_one(json!({"_id": "c", "tags": "rust", "nums": [], "items": []}))
        .unwrap();

    assert_eq!(
        ids(&find(&c, json!({"tags": {"$all": ["rust", "lmdb"]}}))),
        vec!["a"]
    );
    assert_eq!(
        ids(&find(&c, json!({"tags": {"$all": ["rust"]}}))),
        vec!["a", "b", "c"]
    );
    assert!(find(&c, json!({"tags": {"$all": []}})).is_empty());
    assert_eq!(ids(&find(&c, json!({"nums": {"$size": 3}}))), vec!["a"]);
    assert_eq!(ids(&find(&c, json!({"nums": {"$size": 0}}))), vec!["c"]);
    assert_eq!(
        ids(&find(
            &c,
            json!({"nums": {"$elemMatch": {"$gte": 2, "$lt": 4}}})
        )),
        vec!["a"]
    );
    assert_eq!(
        ids(&find(
            &c,
            json!({"items": {"$elemMatch": {"sku": "x", "qty": {"$gt": 5}}}})
        )),
        vec!["b"]
    );
    assert_eq!(
        ids(&find(
            &c,
            json!({"items": {"$elemMatch": {"$and": [{"sku": "y"}, {"qty": 9}]}}})
        )),
        vec!["a"]
    );
    assert!(find(&c, json!({"items": {"$elemMatch": {}}})).len() == 2);
}

#[test]
fn scalar_fields_and_missing_values() {
    let (_dir, store) = store();
    let c = store.collection("scalars").unwrap();
    c.insert_one(json!({"_id": "s", "v": "x"})).unwrap();
    c.insert_one(json!({"_id": "n", "v": 7})).unwrap();
    c.insert_one(json!({"_id": "z", "v": null})).unwrap();

    assert_eq!(ids(&find(&c, json!({"v": {"$all": ["x"]}}))), vec!["s"]);
    assert!(find(&c, json!({"v": {"$size": 1}})).is_empty());
    assert!(find(&c, json!({"missing": {"$all": ["x"]}})).is_empty());
    assert!(find(&c, json!({"missing": {"$elemMatch": {}}})).is_empty());
    assert_eq!(
        ids(&find(&c, json!({"v": {"$not": {"$size": 1}}}))),
        vec!["n", "s", "z"]
    );
}

#[test]
fn single_value_all_can_use_an_index() {
    let (_dir, store) = store();
    let c = store.collection("indexed_all").unwrap();
    c.create_index("kind", false).unwrap();
    c.insert_one(json!({"_id": "a", "kind": "x"})).unwrap();
    c.insert_one(json!({"_id": "b", "kind": "y"})).unwrap();
    c.insert_one(json!({"_id": "c", "kind": "x"})).unwrap();
    assert_eq!(
        ids(&find(&c, json!({"kind": {"$all": ["x"]}}))),
        vec!["a", "c"]
    );
    assert_eq!(c.count(&json!({"kind": {"$all": ["x"]}})).unwrap(), 2);
}

#[test]
fn invalid_array_operator_arguments_are_rejected() {
    let (_dir, store) = store();
    let c = store.collection("invalid_arrays").unwrap();
    c.insert_one(json!({"_id": "a", "v": [1, 2]})).unwrap();
    let invalids = [
        json!({"v": {"$all": "x"}}),
        json!({"v": {"$size": -1}}),
        json!({"v": {"$size": 1.5}}),
        json!({"v": {"$size": "2"}}),
        json!({"v": {"$elemMatch": 1}}),
        json!({"v": {"$elemMatch": {"$regex": "("}}}),
    ];
    for filter in invalids {
        let err = c
            .find(&FindQuery {
                filter: filter.clone(),
                ..Default::default()
            })
            .unwrap_err();
        assert!(matches!(err, Error::Query(_)), "{filter}: {err:?}");
        let err = c.count(&filter).unwrap_err();
        assert!(matches!(err, Error::Query(_)), "{filter}: {err:?}");
        let err = c
            .update(&filter, &json!({"$set": {"x": 1}}), true)
            .unwrap_err();
        assert!(matches!(err, Error::Query(_)), "{filter}: {err:?}");
    }
    assert_eq!(c.count(&json!({})).unwrap(), 1);
    assert!(c
        .find_one(&json!({"_id": "a"}))
        .unwrap()
        .unwrap()
        .get("x")
        .is_none());
}

#[test]
fn array_operators_work_in_aggregation_and_search_filters() {
    let (_dir, store) = store();
    let c = store.collection("array_filters").unwrap();
    c.insert_one(json!({"_id": "a", "title": "rust", "tags": ["db", "fast"]}))
        .unwrap();
    c.insert_one(json!({"_id": "b", "title": "rust", "tags": ["web"]}))
        .unwrap();
    c.create_text_index("title").unwrap();

    let out = c
        .aggregate(&[
            json!({"$match": {"tags": {"$all": ["db", "fast"]}}}),
            json!({"$count": "n"}),
        ])
        .unwrap();
    assert_eq!(out, vec![json!({"n": 1})]);

    let hits = c
        .search("title", "rust", &json!({"tags": {"$size": 1}}), 0, Some(10))
        .unwrap();
    assert_eq!(ids(&hits), vec!["b"]);
}
