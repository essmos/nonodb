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

fn binary(base64: &str) -> serde_json::Value {
    json!({"$binary": {"base64": base64, "subType": "00"}})
}

#[test]
fn extended_integer_forms_are_canonical_and_typed() {
    let (_dir, store) = store();
    let c = store.collection("numbers").unwrap();
    c.insert_one(json!({"_id": "i", "n": {"$numberInt": "42"}}))
        .unwrap();
    c.insert_one(json!({"_id": "l", "n": {"$numberLong": "9007199254740993"}}))
        .unwrap();
    c.insert_one(json!({"_id": "d", "n": {"$numberDouble": "1.5"}}))
        .unwrap();
    c.insert_one(json!({"_id": "x", "n": {"$numberDecimal": "1.50"}}))
        .unwrap();

    let docs = find(&c, json!({"_id": "i"}));
    assert_eq!(docs[0]["n"], json!({"$numberInt": "42"}));
    let docs = find(&c, json!({"_id": "l"}));
    assert_eq!(docs[0]["n"], json!({"$numberLong": "9007199254740993"}));

    assert_eq!(ids(&find(&c, json!({"n": {"$type": "int"}}))), vec!["i"]);
    assert_eq!(ids(&find(&c, json!({"n": {"$type": "long"}}))), vec!["l"]);
    assert_eq!(ids(&find(&c, json!({"n": {"$type": "double"}}))), vec!["d"]);
    assert_eq!(
        ids(&find(&c, json!({"n": {"$type": "decimal"}}))),
        vec!["x"]
    );
    assert_eq!(find(&c, json!({"n": {"$type": "number"}})).len(), 4);

    let docs = find(
        &c,
        json!({"n": {"$gte": {"$numberLong": "9007199254740993"}}}),
    );
    assert_eq!(ids(&docs), vec!["l"]);

    c.update(&json!({"_id": "i"}), &json!({"$inc": {"n": 1}}), false)
        .unwrap();
    assert_eq!(
        find(&c, json!({"_id": "i"}))[0]["n"],
        json!({"$numberInt": "43"})
    );
}

#[test]
fn number_long_index_range_and_inc_are_exact() {
    let (_dir, store) = store();
    let c = store.collection("numbers").unwrap();
    c.create_index("n", false).unwrap();
    for n in ["9007199254740992", "9007199254740993", "9007199254740994"] {
        c.insert_one(json!({"_id": n, "n": {"$numberLong": n}}))
            .unwrap();
    }
    let filter = json!({"n": {"$gte": {"$numberLong": "9007199254740993"}}});
    assert_eq!(
        ids(&find(&c, filter.clone())),
        vec!["9007199254740993", "9007199254740994"]
    );
    assert_eq!(c.count(&filter).unwrap(), 2);

    c.update(
        &json!({"_id": "9007199254740993"}),
        &json!({"$inc": {"n": {"$numberLong": "1"}}}),
        false,
    )
    .unwrap();
    let docs = find(&c, json!({"_id": "9007199254740993"}));
    assert_eq!(docs[0]["n"], json!({"$numberLong": "9007199254740994"}));
}

#[test]
fn binary_uuid_storage_queries_indexes_and_unique_constraints() {
    let (_dir, store) = store();
    let c = store.collection("blobs").unwrap();
    c.create_index("data", false).unwrap();
    c.insert_one(json!({
        "_id": "uuid",
        "data": {"$uuid": "00112233-4455-6677-8899-aabbccddeeff"}
    }))
    .unwrap();
    c.insert_one(json!({"_id": "a", "data": binary("YQ==")}))
        .unwrap();
    c.insert_one(json!({"_id": "b", "data": binary("Yg==")}))
        .unwrap();

    let docs = find(&c, json!({"_id": "uuid"}));
    assert_eq!(docs[0]["data"]["$binary"]["subType"], "04");
    assert_eq!(
        docs[0]["data"]["$binary"]["base64"],
        "ABEiM0RVZneImaq7zN3u/w=="
    );

    assert_eq!(
        ids(&find(
            &c,
            json!({"data": {"$uuid": "00112233-4455-6677-8899-aabbccddeeff"}})
        )),
        vec!["uuid"]
    );
    assert_eq!(find(&c, json!({"data": {"$type": "binData"}})).len(), 3);
    assert_eq!(
        ids(&find(
            &c,
            json!({"data": {"$gt": binary("YQ=="), "$lt": binary("Yw==")}})
        )),
        vec!["b"]
    );

    let u = store.collection("unique_blobs").unwrap();
    u.create_index("data", true).unwrap();
    u.insert_one(json!({
        "_id": "first",
        "data": {"$uuid": "00112233-4455-6677-8899-aabbccddeeff"}
    }))
    .unwrap();
    let err = u
        .insert_one(json!({
            "_id": "dupe",
            "data": {"$uuid": "00112233-4455-6677-8899-aabbccddeeff"}
        }))
        .unwrap_err();
    assert!(matches!(err, nonodb_core::Error::DuplicateKey(_)));
}

#[test]
fn object_id_storage_queries_indexes_and_sorting() {
    let (_dir, store) = store();
    let c = store.collection("refs").unwrap();
    c.create_index("ref", false).unwrap();
    for (id, oid) in [
        ("a", "000000000000000000000001"),
        ("b", "000000000000000000000002"),
        ("c", "000000000000000000000003"),
    ] {
        c.insert_one(json!({"_id": id, "ref": {"$oid": oid}}))
            .unwrap();
    }

    let docs = find(&c, json!({"ref": {"$oid": "000000000000000000000002"}}));
    assert_eq!(ids(&docs), vec!["b"]);
    assert_eq!(find(&c, json!({"ref": {"$type": "objectId"}})).len(), 3);

    let docs = c
        .find(&FindQuery {
            sort: vec![("ref".to_string(), -1)],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(ids(&docs), vec!["c", "b", "a"]);

    let docs = find(
        &c,
        json!({"ref": {"$gte": {"$oid": "000000000000000000000002"}}}),
    );
    assert_eq!(ids(&docs), vec!["b", "c"]);
}

#[test]
fn invalid_binary_and_object_id_are_rejected() {
    let (_dir, store) = store();
    let c = store.collection("bad").unwrap();
    for value in [
        json!({"$binary": {"base64": "!!!", "subType": "00"}}),
        json!({"$binary": {"base64": "YQ==", "subType": "zz"}}),
        json!({"$uuid": "not-a-uuid"}),
    ] {
        let err = c.insert_one(json!({"_id": "bad", "v": value})).unwrap_err();
        assert!(matches!(err, nonodb_core::Error::InvalidBinary(_)));
    }
    let err = c
        .insert_one(json!({"_id": "bad", "v": {"$oid": "xyz"}}))
        .unwrap_err();
    assert!(matches!(err, nonodb_core::Error::InvalidObjectId(_)));
    assert!(find(&c, json!({"_id": "bad"})).is_empty());
}

#[test]
fn type_operator_handles_codes_and_arrays() {
    let (_dir, store) = store();
    let c = store.collection("types").unwrap();
    c.insert_one(json!({"_id": "arr", "v": ["x", {"$numberInt": "1"}]}))
        .unwrap();
    c.insert_one(json!({"_id": "s", "v": "x"})).unwrap();
    c.insert_one(json!({"_id": "n", "v": {"$numberLong": "1"}}))
        .unwrap();

    assert_eq!(
        ids(&find(&c, json!({"v": {"$type": "array"}}))),
        vec!["arr"]
    );
    assert_eq!(
        ids(&find(&c, json!({"v": {"$type": "string"}}))),
        vec!["arr", "s"]
    );
    assert_eq!(ids(&find(&c, json!({"v": {"$type": 16}}))), vec!["arr"]);
    assert_eq!(ids(&find(&c, json!({"v": {"$type": "long"}}))), vec!["n"]);
}

#[test]
fn extended_types_survive_reopen() {
    let (dir, store) = store();
    {
        let c = store.collection("values").unwrap();
        c.create_index("v", false).unwrap();
        c.insert_one(json!({"_id": "b", "v": binary("YQ==")}))
            .unwrap();
        c.insert_one(json!({
            "_id": "o",
            "v": {"$oid": "000000000000000000000001"}
        }))
        .unwrap();
    }
    drop(store);

    let store = Store::open(dir.path()).unwrap();
    let c = store.collection("values").unwrap();
    assert_eq!(ids(&find(&c, json!({"v": binary("YQ==")}))), vec!["b"]);
    assert_eq!(
        ids(&find(
            &c,
            json!({"v": {"$oid": "000000000000000000000001"}})
        )),
        vec!["o"]
    );
}

#[test]
fn search_indexes_extended_numbers_and_skips_non_text_types() {
    let (_dir, store) = store();
    let c = store.collection("search_types").unwrap();
    c.insert_one(json!({"_id": "long", "v": {"$numberLong": "9007199254740993"}}))
        .unwrap();
    c.insert_one(json!({"_id": "decimal", "v": {"$numberDecimal": "199.99"}}))
        .unwrap();
    c.insert_one(json!({"_id": "nan", "v": {"$numberDouble": "NaN"}}))
        .unwrap();
    c.insert_one(json!({"_id": "date", "v": {"$date": {"$numberLong": "1767225600000"}}}))
        .unwrap();
    c.insert_one(json!({"_id": "oid", "v": {"$oid": "000000000000000000000001"}}))
        .unwrap();
    c.insert_one(json!({"_id": "bin", "v": binary("YQ==")}))
        .unwrap();

    c.create_text_index("v").unwrap();
    assert_eq!(
        ids(&c
            .search("v", "9007199254740993", &json!({}), 0, Some(10))
            .unwrap()),
        vec!["long"]
    );
    assert_eq!(
        ids(&c.search("v", "199.99", &json!({}), 0, Some(10)).unwrap()),
        vec!["decimal"]
    );
    assert_eq!(
        ids(&c.search("v", "nan", &json!({}), 0, Some(10)).unwrap()),
        vec!["nan"]
    );
    assert!(c
        .search("v", "1767225600000", &json!({}), 0, Some(10))
        .unwrap()
        .is_empty());
    assert!(c
        .search("v", "000000000000000000000001", &json!({}), 0, Some(10))
        .unwrap()
        .is_empty());

    c.create_fuzzy_index("v").unwrap();
    let hits = c
        .fuzzy_search("v", "9007199254740994", 1, &json!({}), Some(10))
        .unwrap();
    assert_eq!(ids(&hits), vec!["long"]);
    assert_eq!(hits[0]["_distance"], 1);
}
