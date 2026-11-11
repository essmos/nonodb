use nonodb_core::{FindQuery, Store};
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
fn decimal_values_are_stored_as_extended_json() {
    let (_dir, store) = store();
    let c = store.collection("products").unwrap();
    c.insert_one(json!({"_id": "a", "price": {"$numberDecimal": "199.9900"}}))
        .unwrap();

    let docs = find(&c, json!({"_id": "a"}));
    assert_eq!(docs[0]["price"], json!({"$numberDecimal": "199.9900"}));
}

#[test]
fn invalid_decimal_values_are_rejected() {
    let (_dir, store) = store();
    let c = store.collection("products").unwrap();
    for value in [
        json!({"$numberDecimal": "abc"}),
        json!({"$numberDecimal": 1.5}),
        json!({"$numberDecimal": "NaN"}),
        json!({"$numberDouble": "not-a-number"}),
    ] {
        let err = c
            .insert_one(json!({"_id": "bad", "price": value}))
            .unwrap_err();
        assert!(matches!(err, nonodb_core::Error::InvalidNumeric(_)));
        assert!(find(&c, json!({"_id": "bad"})).is_empty());
    }
}

#[test]
fn large_integers_compare_exactly() {
    let (_dir, store) = store();
    let c = store.collection("numbers").unwrap();
    for n in [
        9_007_199_254_740_992i64,
        9_007_199_254_740_993,
        9_007_199_254_740_994,
    ] {
        c.insert_one(json!({"_id": n.to_string(), "n": n})).unwrap();
    }

    let docs = find(&c, json!({"n": 9_007_199_254_740_993i64}));
    assert_eq!(ids(&docs), vec!["9007199254740993"]);

    let docs = find(&c, json!({"n": {"$gt": 9_007_199_254_740_992i64}}));
    assert_eq!(ids(&docs), vec!["9007199254740993", "9007199254740994"]);
}

#[test]
fn large_integer_index_is_exact_and_supports_ranges() {
    let (_dir, store) = store();
    let c = store.collection("numbers").unwrap();
    c.create_index("n", false).unwrap();
    for n in [
        9_007_199_254_740_992i64,
        9_007_199_254_740_993,
        9_007_199_254_740_994,
    ] {
        c.insert_one(json!({"_id": n.to_string(), "n": n})).unwrap();
    }

    let filter = json!({"n": {"$gte": 9_007_199_254_740_993i64, "$lte": 9_007_199_254_740_994i64}});
    let docs = find(&c, filter.clone());
    assert_eq!(ids(&docs), vec!["9007199254740993", "9007199254740994"]);
    assert_eq!(c.count(&filter).unwrap(), 2);

    let docs = find(&c, json!({"n": 9_007_199_254_740_993i64}));
    assert_eq!(ids(&docs), vec!["9007199254740993"]);
}

#[test]
fn int_float_and_decimal_share_one_numeric_bracket() {
    let (_dir, store) = store();
    let c = store.collection("prices").unwrap();
    c.insert_one(json!({"_id": "int", "price": 1})).unwrap();
    c.insert_one(json!({"_id": "float", "price": 1.0})).unwrap();
    c.insert_one(json!({"_id": "decimal", "price": {"$numberDecimal": "1.00"}}))
        .unwrap();
    c.insert_one(json!({"_id": "higher", "price": {"$numberDecimal": "1.5"}}))
        .unwrap();

    let docs = find(&c, json!({"price": {"$numberDecimal": "1"}}));
    assert_eq!(ids(&docs).len(), 3);
    assert!(ids(&docs).contains(&"decimal"));

    let docs = find(&c, json!({"price": {"$gt": {"$numberDecimal": "1.00"}}}));
    assert_eq!(ids(&docs), vec!["higher"]);
}

#[test]
fn decimal_index_range_sort_count_and_unique() {
    let (_dir, store) = store();
    let c = store.collection("products").unwrap();
    c.create_index("price", false).unwrap();
    for (id, price) in [
        ("a", "10.00"),
        ("b", "20.50"),
        ("c", "99.99"),
        ("d", "5.00"),
    ] {
        c.insert_one(json!({"_id": id, "price": {"$numberDecimal": price}}))
            .unwrap();
    }

    let filter =
        json!({"price": {"$gte": {"$numberDecimal": "10"}, "$lt": {"$numberDecimal": "100"}}});
    let docs = find(&c, filter.clone());
    assert_eq!(ids(&docs), vec!["a", "b", "c"]);
    assert_eq!(c.count(&filter).unwrap(), 3);

    let docs = c
        .find(&FindQuery {
            sort: vec![("price".to_string(), -1)],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(ids(&docs), vec!["c", "b", "a", "d"]);

    let u = store.collection("unique_prices").unwrap();
    u.create_index("price", true).unwrap();
    u.insert_one(json!({"_id": "x", "price": {"$numberDecimal": "1.00"}}))
        .unwrap();
    let err = u
        .insert_one(json!({"_id": "y", "price": {"$numberDecimal": "1.0"}}))
        .unwrap_err();
    assert!(matches!(err, nonodb_core::Error::DuplicateKey(_)));
}

#[test]
fn decimal_index_survives_reopen() {
    let (dir, store) = store();
    {
        let c = store.collection("products").unwrap();
        c.create_index("price", false).unwrap();
        c.insert_one(json!({"_id": "a", "price": {"$numberDecimal": "12.50"}}))
            .unwrap();
        c.insert_one(json!({"_id": "b", "price": {"$numberDecimal": "2.50"}}))
            .unwrap();
    }
    drop(store);

    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("products").unwrap();
    let docs = find(&c, json!({"price": {"$gte": {"$numberDecimal": "10"}}}));
    assert_eq!(ids(&docs), vec!["a"]);
}

#[test]
fn compound_index_supports_decimal_range_after_equality() {
    let (_dir, store) = store();
    let c = store.collection("products").unwrap();
    c.create_index_fields(&["tenant".into(), "price".into()], false, &[])
        .unwrap();
    for (id, tenant, price) in [
        ("a", "acme", "5.00"),
        ("b", "acme", "15.00"),
        ("c", "other", "15.00"),
    ] {
        c.insert_one(json!({
            "_id": id,
            "tenant": tenant,
            "price": {"$numberDecimal": price}
        }))
        .unwrap();
    }

    let docs = find(
        &c,
        json!({"tenant": "acme", "price": {"$gte": {"$numberDecimal": "10"}}}),
    );
    assert_eq!(ids(&docs), vec!["b"]);
}

#[test]
fn inc_and_aggregate_arithmetic_keep_decimal_precision() {
    let (_dir, store) = store();
    let c = store.collection("accounts").unwrap();
    c.insert_one(json!({
        "_id": "a",
        "balance": {"$numberDecimal": "10.10"},
        "amount": {"$numberDecimal": "0.20"}
    }))
    .unwrap();

    c.update(
        &json!({"_id": "a"}),
        &json!({"$inc": {"balance": {"$numberDecimal": "0.20"}}}),
        false,
    )
    .unwrap();
    let docs = find(&c, json!({"_id": "a"}));
    assert_eq!(docs[0]["balance"], json!({"$numberDecimal": "10.3"}));

    c.insert_one(json!({"_id": "b", "balance": {"$numberDecimal": "0.20"}}))
        .unwrap();
    let out = c
        .aggregate(&[
            json!({"$group": {"_id": null, "sum": {"$sum": "$balance"}, "avg": {"$avg": "$balance"}}}),
        ])
        .unwrap();
    let sum = out[0]["sum"]["$numberDecimal"].as_str().unwrap();
    let avg = out[0]["avg"]["$numberDecimal"].as_str().unwrap();
    assert_eq!(sum, "10.5");
    assert!(avg.starts_with("5.25"));

    let out = c
        .aggregate(&[
            json!({"$match": {"_id": "a"}}),
            json!({"$project": {
                "_id": 0,
                "total": {"$add": [{"$numberDecimal": "1.10"}, {"$numberDecimal": "2.20"}]},
                "product": {"$multiply": [{"$numberDecimal": "1.5"}, {"$numberDecimal": "2"}]}
            }}),
        ])
        .unwrap();
    assert_eq!(out[0]["total"], json!({"$numberDecimal": "3.3"}));
    assert_eq!(out[0]["product"], json!({"$numberDecimal": "3.0"}));
}

#[test]
fn non_finite_doubles_are_ordered_and_queryable() {
    let (_dir, store) = store();
    let c = store.collection("numbers").unwrap();
    c.create_index("n", false).unwrap();
    for (id, n) in [
        ("nan", json!({"$numberDouble": "NaN"})),
        ("neg_inf", json!({"$numberDouble": "-Infinity"})),
        ("zero", json!(0)),
        ("pos_inf", json!({"$numberDouble": "Infinity"})),
    ] {
        c.insert_one(json!({"_id": id, "n": n})).unwrap();
    }

    let docs = find(&c, json!({"n": {"$numberDouble": "NaN"}}));
    assert_eq!(ids(&docs), vec!["nan"]);
    let docs = c
        .find(&FindQuery {
            filter: json!({"n": {"$gt": {"$numberDouble": "-Infinity"}}}),
            sort: vec![("n".to_string(), 1)],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(ids(&docs), vec!["zero", "pos_inf"]);
    let docs = c
        .find(&FindQuery {
            sort: vec![("n".to_string(), 1)],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(ids(&docs), vec!["nan", "neg_inf", "zero", "pos_inf"]);
}

#[test]
fn upsert_seeds_decimal_equality_filter() {
    let (_dir, store) = store();
    let c = store.collection("products").unwrap();
    c.update_opts(
        &json!({"sku": "A", "price": {"$numberDecimal": "10.50"}}),
        &json!({"$set": {"name": "Widget"}}),
        false,
        true,
    )
    .unwrap();
    let docs = find(&c, json!({"sku": "A"}));
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["price"], json!({"$numberDecimal": "10.50"}));
}
