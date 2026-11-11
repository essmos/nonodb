use nonodb_core::{FindAndModifyOptions, Store};
use serde_json::json;
use tempfile::TempDir;

fn store() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (dir, store)
}

fn opts<'a>(
    filter: &'a serde_json::Value,
    update: Option<&'a serde_json::Value>,
    sort: &'a [(String, i32)],
) -> FindAndModifyOptions<'a> {
    FindAndModifyOptions {
        filter,
        update,
        sort,
        new: false,
        upsert: false,
        remove: false,
        array_filters: &[],
        projection: None,
    }
}

#[test]
fn returns_old_or_new_document() {
    let (_dir, store) = store();
    let c = store.collection("fam").unwrap();
    c.insert_one(json!({"_id": "a", "n": 1})).unwrap();

    let filter = json!({"_id": "a"});
    let update = json!({"$inc": {"n": 1}});
    let out = c
        .find_and_modify(&opts(&filter, Some(&update), &[]))
        .unwrap();
    assert_eq!(out.value, Some(json!({"_id": "a", "n": 1})));
    assert_eq!(out.matched, 1);
    assert_eq!(out.modified, 1);

    let mut o = opts(&filter, Some(&update), &[]);
    o.new = true;
    let out = c.find_and_modify(&o).unwrap();
    assert_eq!(out.value, Some(json!({"_id": "a", "n": 3})));
    assert_eq!(c.find_one(&filter).unwrap().unwrap()["n"], 3);
}

#[test]
fn sort_selects_the_document_and_projection_applies() {
    let (_dir, store) = store();
    let c = store.collection("fam_sort").unwrap();
    for doc in [
        json!({"_id": "a", "score": 1, "secret": "x"}),
        json!({"_id": "b", "score": 9, "secret": "y"}),
        json!({"_id": "c", "score": 5, "secret": "z"}),
    ] {
        c.insert_one(doc).unwrap();
    }

    let filter = json!({});
    let update = json!({"$set": {"picked": true}});
    let sort = vec![("score".to_string(), -1)];
    let mut o = opts(&filter, Some(&update), &sort);
    o.new = true;
    let projection = json!({"_id": 1, "score": 1, "picked": 1});
    o.projection = Some(&projection);
    let out = c.find_and_modify(&o).unwrap();
    assert_eq!(
        out.value,
        Some(json!({"_id": "b", "score": 9, "picked": true}))
    );
    assert!(c.find_one(&json!({"_id": "b"})).unwrap().unwrap()["picked"] == true);
    assert!(c
        .find_one(&json!({"_id": "c"}))
        .unwrap()
        .unwrap()
        .get("picked")
        .is_none());
}

#[test]
fn upsert_and_remove() {
    let (_dir, store) = store();
    let c = store.collection("fam_upsert").unwrap();
    let filter = json!({"_id": "new", "kind": "x"});
    let update = json!({"$set": {"n": 1}});
    let mut o = opts(&filter, Some(&update), &[]);
    o.upsert = true;
    o.new = true;
    let out = c.find_and_modify(&o).unwrap();
    assert_eq!(out.upserted_id.as_deref(), Some("new"));
    assert_eq!(out.value, Some(json!({"_id": "new", "kind": "x", "n": 1})));

    let remove = json!({"_id": "new"});
    let mut o = opts(&remove, None, &[]);
    o.remove = true;
    let out = c.find_and_modify(&o).unwrap();
    assert_eq!(out.value, Some(json!({"_id": "new", "kind": "x", "n": 1})));
    assert_eq!(out.modified, 1);
    assert!(c.find_one(&remove).unwrap().is_none());
}

#[test]
fn array_filters_and_invalid_options() {
    let (_dir, store) = store();
    let c = store.collection("fam_arrays").unwrap();
    c.insert_one(json!({
        "_id": "a",
        "items": [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 2}]
    }))
    .unwrap();
    let filter = json!({"_id": "a"});
    let update = json!({"$inc": {"items.$[i].qty": 10}});
    let filters = vec![json!({"i.sku": "y"})];
    let mut o = opts(&filter, Some(&update), &[]);
    o.new = true;
    o.array_filters = &filters;
    let out = c.find_and_modify(&o).unwrap();
    assert_eq!(
        out.value,
        Some(json!({
            "_id": "a",
            "items": [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 12}]
        }))
    );

    let mut bad = opts(&filter, None, &[]);
    bad.remove = true;
    bad.upsert = true;
    assert!(c.find_and_modify(&bad).is_err());

    let mut bad = opts(&filter, Some(&update), &[]);
    bad.remove = true;
    assert!(c.find_and_modify(&bad).is_err());

    let bad_array_filter = vec![json!({"j.sku": "y"})];
    let mut bad = opts(&filter, Some(&update), &[]);
    bad.array_filters = &bad_array_filter;
    assert!(c.find_and_modify(&bad).is_err());
    assert_eq!(c.find_one(&filter).unwrap().unwrap()["items"][1]["qty"], 12);
}

#[test]
fn changes_survive_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        let c = store.collection("fam_reopen").unwrap();
        c.insert_one(json!({"_id": "a", "n": 1})).unwrap();
        let filter = json!({"_id": "a"});
        let update = json!({"$inc": {"n": 1}});
        let mut o = opts(&filter, Some(&update), &[]);
        o.new = true;
        c.find_and_modify(&o).unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("fam_reopen").unwrap();
    assert_eq!(c.find_one(&json!({"_id": "a"})).unwrap().unwrap()["n"], 2);
}
