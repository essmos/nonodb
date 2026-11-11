use nonodb_core::{Error, Store};
use serde_json::json;
use tempfile::TempDir;

fn store() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (dir, store)
}

#[test]
fn positional_and_all_updates() {
    let (_dir, store) = store();
    let c = store.collection("updates").unwrap();
    c.insert_one(json!({
        "_id": "a",
        "items": [{"sku": "x", "qty": 2}, {"sku": "y", "qty": 9}]
    }))
    .unwrap();

    c.update(
        &json!({"items": {"$elemMatch": {"sku": "y"}}}),
        &json!({"$set": {"items.$.qty": 5}}),
        false,
    )
    .unwrap();
    let doc = c.find_one(&json!({"_id": "a"})).unwrap().unwrap();
    assert_eq!(doc["items"][1]["qty"], 5);
    assert_eq!(doc["items"][0]["qty"], 2);

    c.update(
        &json!({"_id": "a"}),
        &json!({"$inc": {"items.$[].qty": 1}}),
        false,
    )
    .unwrap();
    let doc = c.find_one(&json!({"_id": "a"})).unwrap().unwrap();
    assert_eq!(doc["items"][0]["qty"], 3);
    assert_eq!(doc["items"][1]["qty"], 6);
}

#[test]
fn array_filters_select_matching_elements() {
    let (_dir, store) = store();
    let c = store.collection("filters").unwrap();
    c.insert_one(json!({
        "_id": "a",
        "items": [
            {"sku": "x", "qty": 2},
            {"sku": "y", "qty": 9},
            {"sku": "x", "qty": 10}
        ]
    }))
    .unwrap();
    c.update_opts_filtered(
        &json!({"_id": "a"}),
        &json!({"$set": {"items.$[i].checked": true}}),
        false,
        false,
        &[json!({"i.sku": "x", "i.qty": {"$lt": 5}})],
    )
    .unwrap();
    let doc = c.find_one(&json!({"_id": "a"})).unwrap().unwrap();
    assert_eq!(doc["items"][0]["checked"], true);
    assert!(doc["items"][1].get("checked").is_none());
    assert!(doc["items"][2].get("checked").is_none());
}

#[test]
fn push_modifiers_and_pull_predicates() {
    let (_dir, store) = store();
    let c = store.collection("push").unwrap();
    c.insert_one(json!({"_id": "a", "tags": ["b"], "nums": [1, 5, 10]}))
        .unwrap();

    c.update(
        &json!({"_id": "a"}),
        &json!({"$push": {"tags": {
            "$each": ["a", "c"],
            "$sort": 1,
            "$slice": 2
        }}}),
        false,
    )
    .unwrap();
    let doc = c.find_one(&json!({"_id": "a"})).unwrap().unwrap();
    assert_eq!(doc["tags"], json!(["a", "b"]));

    c.update(
        &json!({"_id": "a"}),
        &json!({"$pull": {"nums": {"$gte": 5}}}),
        false,
    )
    .unwrap();
    let doc = c.find_one(&json!({"_id": "a"})).unwrap().unwrap();
    assert_eq!(doc["nums"], json!([1]));

    c.update(
        &json!({"_id": "a"}),
        &json!({"$push": {"nums": {
            "$each": [2, 3],
            "$position": 1
        }}}),
        false,
    )
    .unwrap();
    let doc = c.find_one(&json!({"_id": "a"})).unwrap().unwrap();
    assert_eq!(doc["nums"], json!([1, 2, 3]));
}

#[test]
fn invalid_array_updates_do_not_mutate() {
    let (_dir, store) = store();
    let c = store.collection("invalid_updates").unwrap();
    c.insert_one(json!({"_id": "a", "items": [{"sku": "x"}, {"sku": "y"}]}))
        .unwrap();

    let err = c
        .update_opts_filtered(
            &json!({"_id": "a"}),
            &json!({"$set": {"items.$[i].qty": 1}}),
            false,
            false,
            &[json!({"j.sku": "x"})],
        )
        .unwrap_err();
    assert!(matches!(err, Error::Query(_)), "{err:?}");

    let err = c
        .update(
            &json!({"_id": "a"}),
            &json!({"$push": {"items": {"$position": 1}}}),
            false,
        )
        .unwrap_err();
    assert!(matches!(err, Error::Query(_)), "{err:?}");

    let doc = c.find_one(&json!({"_id": "a"})).unwrap().unwrap();
    assert_eq!(doc["items"], json!([{"sku": "x"}, {"sku": "y"}]));
}

#[test]
fn array_updates_survive_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        let c = store.collection("reopen_updates").unwrap();
        c.insert_one(json!({
            "_id": "a",
            "items": [{"sku": "x", "qty": 2}, {"sku": "y", "qty": 9}]
        }))
        .unwrap();
        c.update_opts_filtered(
            &json!({"_id": "a"}),
            &json!({"$inc": {"items.$[i].qty": 1}}),
            false,
            false,
            &[json!({"i.sku": "y"})],
        )
        .unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("reopen_updates").unwrap();
    let doc = c.find_one(&json!({"_id": "a"})).unwrap().unwrap();
    assert_eq!(doc["items"][1]["qty"], 10);
}
