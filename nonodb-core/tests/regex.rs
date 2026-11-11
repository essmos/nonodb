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
fn regex_queries_strings_arrays_options_and_logic() {
    let (_dir, store) = store();
    let c = store.collection("people").unwrap();
    c.insert_one(json!({"_id": "a", "name": "Ala", "tags": ["kot", "pies"]}))
        .unwrap();
    c.insert_one(json!({"_id": "b", "name": "Alan", "tags": ["ptak"]}))
        .unwrap();
    c.insert_one(json!({"_id": "c", "name": "Bob", "tags": ["pies"]}))
        .unwrap();

    assert_eq!(
        ids(&find(&c, json!({"name": {"$regex": "^Al"}}))),
        vec!["a", "b"]
    );
    assert_eq!(
        ids(&find(
            &c,
            json!({"name": {"$regex": "^alan$", "$options": "i"}})
        )),
        vec!["b"]
    );
    assert_eq!(
        ids(&find(&c, json!({"tags": {"$regex": "^pi"}}))),
        vec!["a", "c"]
    );
    assert_eq!(
        ids(&find(&c, json!({"name": {"$not": {"$regex": "^Al"}}}))),
        vec!["c"]
    );
    assert_eq!(
        ids(&find(
            &c,
            json!({"$or": [
                {"name": {"$regex": "^Al"}},
                {"name": {"$regex": "^Bo"}}
            ]})
        )),
        vec!["a", "b", "c"]
    );
    assert_eq!(c.count(&json!({"name": {"$regex": "^Al"}})).unwrap(), 2);
}

#[test]
fn regex_ignores_non_strings_missing_fields_and_dates() {
    let (_dir, store) = store();
    let c = store.collection("values").unwrap();
    c.insert_one(json!({"_id": "n", "v": 42})).unwrap();
    c.insert_one(json!({"_id": "b", "v": true})).unwrap();
    c.insert_one(json!({"_id": "null", "v": null})).unwrap();
    c.insert_one(json!({"_id": "date", "v": {"$date": {"$numberLong": "0"}}}))
        .unwrap();
    c.insert_one(json!({"_id": "arr", "v": [1, 2, 3]})).unwrap();
    c.insert_one(json!({"_id": "objs", "v": [{"x": "abc"}]}))
        .unwrap();

    assert!(find(&c, json!({"v": {"$regex": "42"}})).is_empty());
    assert!(find(&c, json!({"v": {"$regex": "true"}})).is_empty());
    assert!(find(&c, json!({"v": {"$regex": ".*"}})).is_empty());
    assert!(find(&c, json!({"missing": {"$regex": ".*"}})).is_empty());
}

#[test]
fn anchored_regex_uses_string_index_prefix() {
    let (_dir, store) = store();
    let c = store.collection("indexed").unwrap();
    c.create_index("name", false).unwrap();
    for (id, name) in [("a", "Ala"), ("b", "Alan"), ("c", "Bob"), ("d", "Al")] {
        c.insert_one(json!({"_id": id, "name": name})).unwrap();
    }
    assert_eq!(
        ids(&find(&c, json!({"name": {"$regex": "^Al"}}))),
        vec!["a", "b", "d"]
    );
    assert_eq!(c.count(&json!({"name": {"$regex": "^Al"}})).unwrap(), 3);
    assert!(find(&c, json!({"name": {"$regex": "^Al", "$options": "i"}})).len() == 3);

    // Compound prefix: equality on the first field, regex on the last.
    let cc = store.collection("compound").unwrap();
    cc.create_index_fields(&["tenant".to_string(), "name".to_string()], false, &[])
        .unwrap();
    cc.insert_one(json!({"tenant": "x", "name": "Ala"}))
        .unwrap();
    cc.insert_one(json!({"tenant": "x", "name": "Bob"}))
        .unwrap();
    cc.insert_one(json!({"tenant": "y", "name": "Ala"}))
        .unwrap();
    assert_eq!(
        find(&cc, json!({"tenant": "x", "name": {"$regex": "^Al"}})).len(),
        1
    );
}

#[test]
fn invalid_regex_is_rejected_before_writes() {
    let (_dir, store) = store();
    let c = store.collection("invalid").unwrap();
    c.insert_one(json!({"_id": "a", "name": "Ala"})).unwrap();

    for err in [
        c.find(&FindQuery {
            filter: json!({"name": {"$regex": "("}}),
            ..Default::default()
        })
        .unwrap_err(),
        c.count(&json!({"name": {"$regex": "("}})).unwrap_err(),
        c.update(
            &json!({"name": {"$regex": "("}}),
            &json!({"$set": {"x": 1}}),
            true,
        )
        .unwrap_err(),
        c.delete(&json!({"name": {"$regex": "("}}), true)
            .unwrap_err(),
        c.aggregate(&[json!({"$match": {"name": {"$regex": "("}}})])
            .unwrap_err(),
    ] {
        assert!(matches!(err, Error::Query(_)), "{err:?}");
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
fn regex_accepts_extended_expression_form() {
    let (_dir, store) = store();
    let c = store.collection("extended").unwrap();
    c.insert_one(json!({"_id": "a", "name": "Ala"})).unwrap();
    c.insert_one(json!({"_id": "b", "name": "Bob"})).unwrap();
    let filter = json!({"name": {"$regex": {"$regularExpression": {
        "pattern": "^ala$", "options": "i"
    }}}});
    assert_eq!(ids(&find(&c, filter)), vec!["a"]);
}
