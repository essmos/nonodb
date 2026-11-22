//! Unknown filter operators must fail loudly instead of matching nothing.

use nonodb_core::{Error, FindQuery, Store};
use serde_json::json;
use tempfile::TempDir;

fn seeded() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("operators").unwrap();
    for n in 0..5 {
        c.insert_one(json!({"_id": format!("d{n}"), "n": n, "tags": ["x"]}))
            .unwrap();
    }
    (dir, store)
}

fn find_error(c: &nonodb_core::Collection<'_>, filter: serde_json::Value) -> String {
    match c.find(&FindQuery {
        filter,
        ..Default::default()
    }) {
        Ok(_) => panic!("filter was accepted"),
        Err(Error::Query(message)) => message,
        Err(error) => panic!("unexpected error: {error}"),
    }
}

#[test]
fn unknown_field_operator_is_rejected() {
    let (_dir, store) = seeded();
    let c = store.collection("operators").unwrap();
    let message = find_error(&c, json!({"tags": {"$bogusOp": "x"}}));
    assert!(message.contains("unknown operator: $bogusOp"), "{message}");
    assert!(c.count(&json!({"tags": {"$bogusOp": "x"}})).is_err());
}

#[test]
fn unknown_top_level_operator_is_rejected() {
    let (_dir, store) = seeded();
    let c = store.collection("operators").unwrap();
    let message = find_error(&c, json!({"$bogus": []}));
    assert!(
        message.contains("unknown top level operator: $bogus"),
        "{message}"
    );
}

#[test]
fn unknown_operator_inside_expr_is_rejected() {
    let (_dir, store) = seeded();
    let c = store.collection("operators").unwrap();
    let message = find_error(&c, json!({"$expr": {"$eq": [{"$bogus": "$n"}, 1]}}));
    assert!(
        message.contains("unsupported aggregation expression `$bogus`"),
        "{message}"
    );

    // Not evaluated on a real document, so a walker is required to see this.
    let message = find_error(&c, json!({"$expr": {"$cond": [true, 1, {"$bogus": "$n"}]}}));
    assert!(
        message.contains("unsupported aggregation expression `$bogus`"),
        "{message}"
    );
}

#[test]
fn negation_cannot_turn_an_unknown_operator_into_every_document() {
    let (_dir, store) = seeded();
    let c = store.collection("operators").unwrap();
    let filter = json!({"$nor": [{"tags": {"$bogusOp": "x"}}]});
    assert!(c
        .find(&FindQuery {
            filter: filter.clone(),
            ..Default::default()
        })
        .is_err());

    // The dangerous shape: a silently-false predicate would delete everything.
    assert!(c.delete(&filter, true).is_err());
    assert_eq!(c.count(&json!({})).unwrap(), 5);

    let filter = json!({"n": {"$not": {"$bogusOp": 1}}});
    assert!(c
        .update(&filter, &json!({"$set": {"touched": true}}), true)
        .is_err());
    assert_eq!(c.count(&json!({"touched": true})).unwrap(), 0);
}

#[test]
fn supported_operators_keep_working() {
    let (_dir, store) = seeded();
    let c = store.collection("operators").unwrap();
    let ids = |docs: Vec<serde_json::Value>| -> Vec<String> {
        docs.iter()
            .map(|d| d["_id"].as_str().unwrap().to_string())
            .collect()
    };
    let run = |filter: serde_json::Value| -> Vec<String> {
        ids(c
            .find(&FindQuery {
                filter,
                ..Default::default()
            })
            .unwrap())
    };

    assert_eq!(
        run(json!({"tags": {"$in": ["x"]}, "n": {"$gte": 3}})),
        ["d3", "d4"]
    );
    assert_eq!(run(json!({"tags": {"$size": 1}, "n": {"$lt": 1}})), ["d0"]);
    assert_eq!(
        run(json!({"$and": [{"n": {"$ne": 0}}, {"n": {"$lte": 1}}]})),
        ["d1"]
    );
    assert_eq!(
        run(json!({"$or": [{"_id": "d0"}, {"_id": "d4"}]})),
        ["d0", "d4"]
    );
    assert_eq!(run(json!({"$expr": {"$eq": ["$n", 2]}})), ["d2"]);
    assert_eq!(
        run(json!({"n": {"$not": {"$gt": 3}}})),
        ["d0", "d1", "d2", "d3"]
    );
    // `$literal` carries data, so its content is not validated as an expression.
    assert_eq!(
        run(json!({"$expr": {"$eq": [
            {"$literal": {"$bogus": 1}},
            {"$literal": {"$bogus": 1}}
        ]}})),
        ["d0", "d1", "d2", "d3", "d4"]
    );
    // `$comment` is not a supported filter operator, so it fails loudly instead of
    // silently dropping the rest of the filter.
    assert!(c
        .find(&FindQuery {
            filter: json!({"$comment": "audit", "n": 4}),
            ..Default::default()
        })
        .is_err());
}
