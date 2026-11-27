//! Differential aggregation and SQL tests for the experimental columnar profile.

use nonodb_core::{Collection, CollectionProfile, Store};
use serde_json::{json, Value};

fn seed_docs() -> Vec<Value> {
    vec![
        json!({"_id": "a", "g": "x", "v": 1, "tags": ["r", "b"]}),
        json!({"_id": "b", "g": "x", "v": 2, "tags": ["b"]}),
        json!({"_id": "c", "g": "y", "v": 5, "tags": []}),
        json!({"_id": "d", "g": "y", "v": 8, "tags": ["r"]}),
        json!({"_id": "e", "g": "z", "v": 13, "tags": ["g", "r"]}),
    ]
}

fn setup() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    &'static Store,
    &'static Store,
    Collection<'static>,
    Collection<'static>,
) {
    // Leak the stores so the returned collections can borrow them for the test.
    let sd = tempfile::tempdir().unwrap();
    let cd = tempfile::tempdir().unwrap();
    let ss: &'static Store = Box::leak(Box::new(Store::open(sd.path()).unwrap()));
    let cs: &'static Store = Box::leak(Box::new(Store::open(cd.path()).unwrap()));
    let sc = ss
        .create_collection("docs", CollectionProfile::Standard)
        .unwrap();
    let cc = cs
        .create_columnar_collection("docs", &nonodb_core::columnar::Settings::default())
        .unwrap();
    for doc in seed_docs() {
        sc.insert_one(doc.clone()).unwrap();
        cc.insert_one(doc).unwrap();
    }
    for (store, profile) in [
        (ss, CollectionProfile::Standard),
        (cs, CollectionProfile::Columnar),
    ] {
        let other = if profile == CollectionProfile::Columnar {
            store
                .create_columnar_collection("other", &nonodb_core::columnar::Settings::default())
                .unwrap()
        } else {
            store.create_collection("other", profile).unwrap()
        };
        other.insert_one(json!({"_id": "x", "label": "X"})).unwrap();
        other.insert_one(json!({"_id": "y", "label": "Y"})).unwrap();
    }
    (sd, cd, ss, cs, sc, cc)
}

#[test]
fn columnar_aggregation_matches_standard_for_supported_stages() {
    let (_sd, _cd, _ss, _cs, sc, cc) = setup();
    let pipelines = vec![
        vec![
            json!({"$match": {"v": {"$gte": 2}}}),
            json!({"$sort": {"_id": 1}}),
        ],
        vec![
            json!({"$group": {"_id": "$g", "total": {"$sum": "$v"}, "avg": {"$avg": "$v"}, "n": {"$sum": 1}}}),
            json!({"$sort": {"_id": 1}}),
        ],
        vec![
            json!({"$project": {"_id": 0, "g": 1, "double": {"$multiply": ["$v", 2]}}}),
            json!({"$sort": {"double": -1, "g": 1}}),
        ],
        vec![
            json!({"$sort": {"v": 1}}),
            json!({"$skip": 1}),
            json!({"$limit": 2}),
        ],
        vec![
            json!({"$unwind": "$tags"}),
            json!({"$group": {"_id": "$tags", "n": {"$sum": 1}}}),
            json!({"$sort": {"_id": 1}}),
        ],
        vec![
            json!({"$bucket": {"groupBy": "$v", "boundaries": [0, 5, 10, 20], "default": "other", "output": {"n": {"$sum": 1}}}}),
        ],
        vec![
            json!({"$lookup": {"from": "other", "localField": "g", "foreignField": "_id", "as": "joined"}}),
            json!({"$sort": {"_id": 1}}),
            json!({"$limit": 3}),
        ],
        vec![
            json!({"$unionWith": {"coll": "other"}}),
            json!({"$count": "n"}),
        ],
        vec![
            json!({"$facet": {"by_g": [{"$group": {"_id": "$g", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}], "count": [{"$count": "n"}]}}),
        ],
    ];
    for pipeline in pipelines {
        let expected = sc.aggregate(&pipeline).unwrap();
        let actual = cc.aggregate(&pipeline).unwrap();
        assert_eq!(actual, expected, "pipeline {pipeline:?}");
    }
}

#[test]
fn columnar_sql_reads_and_writes_match_standard() {
    let (_sd, _cd, ss, cs, _sc, _cc) = setup();
    let q = nonodb_core::sql::PreparedSql::parse(
        "SELECT g, SUM(v) AS total, COUNT(*) AS n FROM docs WHERE v >= ? GROUP BY g ORDER BY g",
    )
    .unwrap();
    let params = vec![json!(2)];
    let expected = q.execute(&ss.collection("docs").unwrap(), &params).unwrap();
    let actual = q.execute(&cs.collection("docs").unwrap(), &params).unwrap();
    assert_eq!(actual, expected);

    let insert = nonodb_core::sql::PreparedSql::parse(
        "INSERT INTO docs (_id, g, v, tags) VALUES (?, ?, ?, ?)",
    )
    .unwrap();
    let mut sw = ss.write_txn().unwrap();
    insert
        .execute_write(
            &ss.collection_in(&mut sw, "docs").unwrap(),
            &mut sw,
            &[json!("f"), json!("z"), json!(21), json!(["new"])],
        )
        .unwrap();
    sw.commit().unwrap();
    let mut cw = cs.write_txn().unwrap();
    insert
        .execute_write(
            &cs.collection_in(&mut cw, "docs").unwrap(),
            &mut cw,
            &[json!("f"), json!("z"), json!(21), json!(["new"])],
        )
        .unwrap();
    cw.commit().unwrap();
    assert_eq!(
        cs.collection("docs").unwrap().find_by_key("f").unwrap(),
        ss.collection("docs").unwrap().find_by_key("f").unwrap()
    );

    let update =
        nonodb_core::sql::PreparedSql::parse("UPDATE docs SET v = v + ? WHERE g = ?").unwrap();
    let mut sw = ss.write_txn().unwrap();
    let se = update
        .execute_write(
            &ss.collection_in(&mut sw, "docs").unwrap(),
            &mut sw,
            &[json!(10), json!("z")],
        )
        .unwrap();
    sw.commit().unwrap();
    let mut cw = cs.write_txn().unwrap();
    let ce = update
        .execute_write(
            &cs.collection_in(&mut cw, "docs").unwrap(),
            &mut cw,
            &[json!(10), json!("z")],
        )
        .unwrap();
    cw.commit().unwrap();
    assert_eq!(ce, se);

    let delete = nonodb_core::sql::PreparedSql::parse("DELETE FROM docs WHERE g = ?").unwrap();
    let mut sw = ss.write_txn().unwrap();
    let se = delete
        .execute_write(
            &ss.collection_in(&mut sw, "docs").unwrap(),
            &mut sw,
            &[json!("x")],
        )
        .unwrap();
    sw.commit().unwrap();
    let mut cw = cs.write_txn().unwrap();
    let ce = delete
        .execute_write(
            &cs.collection_in(&mut cw, "docs").unwrap(),
            &mut cw,
            &[json!("x")],
        )
        .unwrap();
    cw.commit().unwrap();
    assert_eq!(ce, se);
    let mut sw = ss.write_txn().unwrap();
    let se = ss
        .collection_in(&mut sw, "docs")
        .unwrap()
        .find_in(
            &sw,
            &nonodb_core::FindQuery {
                sort: vec![("_id".into(), 1)],
                ..Default::default()
            },
        )
        .unwrap();
    sw.commit().unwrap();
    let mut cw = cs.write_txn().unwrap();
    let ce = cs
        .collection_in(&mut cw, "docs")
        .unwrap()
        .find_in(
            &cw,
            &nonodb_core::FindQuery {
                sort: vec![("_id".into(), 1)],
                ..Default::default()
            },
        )
        .unwrap();
    cw.commit().unwrap();
    assert_eq!(ce, se);
}
