//! Differential read-path tests for the experimental columnar profile.

use nonodb_core::{Collection, CollectionProfile, FindAndModifyOptions, FindQuery, Store};
use serde_json::{json, Value};

fn docs() -> Vec<Value> {
    vec![
        json!({"_id": "a", "n": 1, "s": "alpha", "nested": {"x": 1, "y": "one"}, "tags": ["red", "blue"], "flag": true}),
        json!({"_id": "b", "n": 2.5, "s": "Beta", "nested": {"x": 2}, "tags": ["blue"], "flag": false}),
        json!({"_id": "c", "n": 10, "s": "gamma", "nested": {"x": 3, "y": null}, "tags": [], "missing": null}),
        json!({"_id": "d", "n": {"$numberLong": "9007199254740993"}, "s": "delta", "tags": ["red", "green"], "arr": [{"k": 1}, {"k": 2}]}),
        json!({"_id": "e", "n": -4, "s": "epsilon", "nested": {"x": 0}, "tags": ["blue", "red"]}),
        json!({"_id": "f", "s": "zeta", "date": {"$date": {"$numberLong": "1700000000000"}}}),
    ]
}

fn query(
    filter: Value,
    sort: Vec<(&str, i32)>,
    skip: usize,
    limit: Option<usize>,
    projection: Option<Value>,
) -> FindQuery {
    FindQuery {
        filter,
        projection,
        sort: sort
            .into_iter()
            .map(|(field, dir)| (field.to_string(), dir))
            .collect(),
        skip,
        limit,
        comment: None,
        max_time_ms: None,
        hint: None,
    }
}

fn standard(store: &Store) -> Collection<'_> {
    store
        .create_collection("docs", CollectionProfile::Standard)
        .unwrap()
}

fn columnar(store: &Store) -> Collection<'_> {
    store
        .create_columnar_collection("docs", &nonodb_core::columnar::Settings::default())
        .unwrap()
}

#[test]
fn columnar_reads_match_standard_across_filters_projection_sort_and_paging() {
    let sd = tempfile::tempdir().unwrap();
    let cd = tempfile::tempdir().unwrap();
    let ss = Store::open(sd.path()).unwrap();
    let cs = Store::open(cd.path()).unwrap();
    let sc = standard(&ss);
    let cc = columnar(&cs);
    for doc in docs() {
        sc.insert_one(doc.clone()).unwrap();
        cc.insert_one(doc).unwrap();
    }

    let cases = vec![
        query(json!({"n": {"$gte": 1}}), vec![("_id", 1)], 0, None, None),
        query(
            json!({"n": {"$lt": 2}}),
            vec![("n", 1), ("_id", -1)],
            0,
            None,
            None,
        ),
        query(
            json!({"s": {"$regex": "^[ab]", "$options": "i"}}),
            vec![("_id", 1)],
            0,
            None,
            None,
        ),
        query(
            json!({"nested.x": {"$in": [1, 3]}}),
            vec![("_id", 1)],
            0,
            None,
            None,
        ),
        query(json!({"tags": "red"}), vec![("_id", 1)], 0, None, None),
        query(
            json!({"tags": {"$all": ["red", "blue"]}}),
            vec![("_id", 1)],
            0,
            None,
            None,
        ),
        query(
            json!({"tags": {"$size": 0}}),
            vec![("_id", 1)],
            0,
            None,
            None,
        ),
        query(
            json!({"arr": {"$elemMatch": {"k": {"$gt": 1}}}}),
            vec![("_id", 1)],
            0,
            None,
            None,
        ),
        query(
            json!({"$expr": {"$gt": ["$n", 2]}}),
            vec![("_id", 1)],
            0,
            None,
            None,
        ),
        query(
            json!({"missing": {"$exists": false}}),
            vec![("_id", 1)],
            0,
            None,
            None,
        ),
        query(json!({"nested.y": null}), vec![("_id", 1)], 0, None, None),
        query(
            json!({"n": {"$type": "number"}}),
            vec![("n", -1)],
            0,
            None,
            None,
        ),
        query(json!({}), vec![("n", 1), ("_id", 1)], 1, Some(3), None),
        query(
            json!({}),
            vec![("_id", -1)],
            2,
            Some(2),
            Some(json!({"_id": 1, "s": 1})),
        ),
        query(
            json!({}),
            vec![("_id", 1)],
            0,
            None,
            Some(json!({"n": 0, "tags": 0})),
        ),
    ];
    for case in cases {
        let expected = sc.find(&case).unwrap();
        let actual = cc.find(&case).unwrap();
        assert_eq!(actual, expected, "query {:?}", case);
        assert_eq!(
            cc.count(&case.filter).unwrap(),
            sc.count(&case.filter).unwrap()
        );
    }

    for id in ["a", "missing", "f"] {
        assert_eq!(cc.find_by_key(id).unwrap(), sc.find_by_key(id).unwrap());
        assert_eq!(
            cc.find_one(&json!({"_id": id})).unwrap(),
            sc.find_one(&json!({"_id": id})).unwrap()
        );
    }
}

#[test]
fn columnar_sort_ties_break_by_id_like_standard() {
    let sd = tempfile::tempdir().unwrap();
    let cd = tempfile::tempdir().unwrap();
    let ss = Store::open(sd.path()).unwrap();
    let cs = Store::open(cd.path()).unwrap();
    let sc = standard(&ss);
    let cc = columnar(&cs);
    for id in ["z", "m", "a", "q"] {
        let doc = json!({"_id": id, "v": 7});
        sc.insert_one(doc.clone()).unwrap();
        cc.insert_one(doc).unwrap();
    }
    for (id, value) in [
        ("m2", json!(null)),
        ("a2", json!(null)),
        ("z2", json!(null)),
    ] {
        let doc = json!({"_id": id, "v": value});
        sc.insert_one(doc.clone()).unwrap();
        cc.insert_one(doc).unwrap();
    }

    for sort in [vec![("v", 1)], vec![("v", -1)]] {
        for (skip, limit) in [(0, None), (0, Some(2)), (2, Some(2)), (1, Some(3))] {
            let q = query(json!({}), sort.clone(), skip, limit, None);
            let expected = sc.find(&q).unwrap();
            let actual = cc.find(&q).unwrap();
            assert_eq!(
                actual, expected,
                "sort {sort:?} skip {skip} limit {limit:?}"
            );
        }
    }

    let options = FindAndModifyOptions {
        filter: &json!({}),
        update: Some(&json!({"$set": {"picked": true}})),
        sort: &[("v".to_string(), 1)],
        new: false,
        upsert: false,
        remove: false,
        array_filters: &[],
        projection: None,
    };
    let expected = sc.find_and_modify(&options).unwrap().value;
    let actual = cc.find_and_modify(&options).unwrap().value;
    assert_eq!(actual, expected);
    assert_eq!(actual.unwrap()["_id"], json!("a2"));
}

#[test]
fn columnar_lookup_and_explain_match_standard_contracts() {
    let sd = tempfile::tempdir().unwrap();
    let cd = tempfile::tempdir().unwrap();
    let ss = Store::open(sd.path()).unwrap();
    let cs = Store::open(cd.path()).unwrap();
    let sc = standard(&ss);
    let cc = columnar(&cs);
    for doc in docs() {
        sc.insert_one(doc.clone()).unwrap();
        cc.insert_one(doc).unwrap();
    }

    let values = vec![json!("a"), json!("b"), json!("a"), json!("missing")];
    let expected = sc.lookup("_id", &values, Some(&json!({"s": 1}))).unwrap();
    let actual = cc.lookup("_id", &values, Some(&json!({"s": 1}))).unwrap();
    assert_eq!(actual, expected);

    let q = query(
        json!({"n": {"$gte": 2}}),
        vec![("_id", 1)],
        1,
        Some(2),
        None,
    );
    let explained = cc.explain(&q).unwrap();
    assert_eq!(explained["profile"], "columnar");
    assert_eq!(explained["actual_matched"], sc.count(&q.filter).unwrap());
    assert_eq!(explained["actual_returned"], cc.find(&q).unwrap().len());
}

#[test]
fn columnar_ttl_reads_hide_expired_documents() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store
        .create_columnar_collection("docs", &nonodb_core::columnar::Settings::default())
        .unwrap();
    store
        .set_collection_ttl_after("docs", "expireAt", 60, Some(false))
        .unwrap();
    let coll = store.collection("docs").unwrap();
    let now = nonodb_core::ttl::now_ms();
    coll.insert_one(json!({
        "_id": "old",
        "expireAt": {"$date": {"$numberLong": (now - 120_000).to_string()}},
        "value": 1,
    }))
    .unwrap();
    coll.insert_one(json!({
        "_id": "new",
        "expireAt": {"$date": {"$numberLong": (now + 120_000).to_string()}},
        "value": 2,
    }))
    .unwrap();
    assert_eq!(coll.find(&FindQuery::default()).unwrap().len(), 1);
    assert_eq!(coll.count(&json!({})).unwrap(), 1);
    assert_eq!(
        coll.lookup("_id", &[json!("old"), json!("new")], None)
            .unwrap(),
        vec![
            None,
            Some(json!({
                "_id": "new",
                "expireAt": {"$date": {"$numberLong": (now + 120_000).to_string()}},
                "value": 2,
            }))
        ]
    );
}
