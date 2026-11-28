//! Differential write-operator, batch, and id coverage for columnar collections.

use nonodb_core::{Collection, CollectionProfile, Store};
use serde_json::{json, Value};

fn query() -> nonodb_core::FindQuery {
    nonodb_core::FindQuery {
        sort: vec![("_id".into(), 1)],
        ..Default::default()
    }
}

fn setup() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Collection<'static>,
    Collection<'static>,
) {
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
    let doc = json!({
        "_id": "a",
        "n": 5,
        "s": "alpha",
        "arr": [1, 2, 3],
        "items": [{"k": "a", "q": 1}, {"k": "b", "q": 2}],
        "nested": {"x": 1, "y": 2},
        "nullable": null,
    });
    sc.insert_one(doc.clone()).unwrap();
    cc.insert_one(doc).unwrap();
    (sd, cd, sc, cc)
}

fn assert_same(sc: &Collection<'_>, cc: &Collection<'_>, step: &str) {
    assert_eq!(
        sc.find(&query()).unwrap(),
        cc.find(&query()).unwrap(),
        "documents after {step}"
    );
}

#[test]
fn update_operators_and_replacements_match_standard() {
    let (_sd, _cd, sc, cc) = setup();
    let updates = [
        json!({"$inc": {"n": 2, "nested.x": -1}}),
        json!({"$mul": {"n": 3}}),
        json!({"$min": {"n": 4}, "$max": {"nested.y": 10}}),
        json!({"$rename": {"s": "label"}}),
        json!({"$set": {"nullable": 0, "new.deep": {"v": true}}}),
        json!({"$unset": {"nullable": "", "new.deep": ""}}),
        json!({"$push": {"arr": {"$each": [4, 5], "$sort": 1, "$slice": -3}}}),
        json!({"$addToSet": {"arr": {"$each": [5, 6]}}}),
        json!({"$pull": {"arr": {"$gte": 5}}}),
        json!({"$pop": {"arr": -1}}),
        json!({"$set": {"items.$[el].q": 99}}),
    ];
    for (index, update) in updates.iter().enumerate() {
        let filters: &[Value] = if index == updates.len() - 1 {
            &[json!({"items.k": "b"})]
        } else {
            &[json!({"_id": "a"})]
        };
        for filter in filters {
            let array_filters: &[Value] = if index == updates.len() - 1 {
                &[json!({"el.k": "b"})]
            } else {
                &[]
            };
            let result_s = sc
                .update_opts_filtered(filter, update, false, false, array_filters)
                .unwrap();
            let result_c = cc
                .update_opts_filtered(filter, update, false, false, array_filters)
                .unwrap();
            assert_eq!(
                result_s.modified, result_c.modified,
                "step {index} {update}"
            );
            assert_same(&sc, &cc, &format!("{index} {update}"));
        }
    }

    let replacement = json!({"_id": "a", "n": 1, "arr": [], "items": [], "nested": {}});
    sc.update(&json!({"_id": "a"}), &replacement, false)
        .unwrap();
    cc.update(&json!({"_id": "a"}), &replacement, false)
        .unwrap();
    assert_same(&sc, &cc, "replacement");
}

#[test]
fn invalid_id_and_duplicate_errors_match_standard() {
    let (_sd, _cd, sc, cc) = setup();
    for id in [json!(42), json!(true), json!(["array"])] {
        let doc = json!({"_id": id, "n": 1});
        let se = sc.insert_one(doc.clone()).unwrap_err().to_string();
        let ce = cc.insert_one(doc).unwrap_err().to_string();
        assert_eq!(se, ce, "insert invalid id {id}");
    }
    let doc = json!({"_id": "a", "n": 99});
    let se = sc.insert_one(doc.clone()).unwrap_err().to_string();
    let ce = cc.insert_one(doc).unwrap_err().to_string();
    assert_eq!(se, ce, "duplicate insert");
    let operator = json!({"$set": {"_id": "b"}});
    let se = sc
        .update(&json!({"_id": "a"}), &operator, false)
        .unwrap_err()
        .to_string();
    let ce = cc
        .update(&json!({"_id": "a"}), &operator, false)
        .unwrap_err()
        .to_string();
    assert_eq!(se, ce, "operator id change");
    let replacement = json!({"_id": "b", "n": 1});
    assert_eq!(
        sc.update(&json!({"_id": "a"}), &replacement, false)
            .unwrap(),
        cc.update(&json!({"_id": "a"}), &replacement, false)
            .unwrap()
    );
    assert_eq!(
        sc.find_by_key("a").unwrap().unwrap()["_id"],
        cc.find_by_key("a").unwrap().unwrap()["_id"]
    );
    assert_same(&sc, &cc, "invalid id attempts");
}

#[test]
fn insert_many_order_duplicate_atomicity_and_bson_ids_match_standard() {
    let sd = tempfile::tempdir().unwrap();
    let cd = tempfile::tempdir().unwrap();
    let ss = Store::open(sd.path()).unwrap();
    let cs = Store::open(cd.path()).unwrap();
    let sc = ss
        .create_collection("docs", CollectionProfile::Standard)
        .unwrap();
    let cc = cs
        .create_columnar_collection("docs", &nonodb_core::columnar::Settings::default())
        .unwrap();
    let docs = vec![
        json!({"_id": "b", "n": 2}),
        json!({"_id": "a", "n": 1}),
        json!({"_id": {"$oid": "0123456789abcdef01234567"}, "n": 3}),
    ];
    let mut sw = ss.write_txn().unwrap();
    let ids_s = sc.insert_many_txn(&mut sw, &docs).unwrap();
    sw.commit().unwrap();
    let mut cw = cs.write_txn().unwrap();
    let ids_c = cc.insert_many_txn(&mut cw, &docs).unwrap();
    cw.commit().unwrap();
    assert_eq!(ids_s, ids_c);
    assert_same(&sc, &cc, "insert_many");

    let duplicate = vec![json!({"_id": "fresh"}), json!({"_id": "a"})];
    let mut sw = ss.write_txn().unwrap();
    let err_s = sc.insert_many_txn(&mut sw, &duplicate).unwrap_err();
    drop(sw);
    let mut cw = cs.write_txn().unwrap();
    let err_c = cc.insert_many_txn(&mut cw, &duplicate).unwrap_err();
    drop(cw);
    assert_eq!(err_s.to_string(), err_c.to_string());
    assert!(sc.find_by_key("fresh").unwrap().is_none());
    assert!(cc.find_by_key("fresh").unwrap().is_none());

    let duplicate_in_batch = vec![
        json!({"_id": "twice", "n": 1}),
        json!({"_id": "twice", "n": 2}),
    ];
    let mut sw = ss.write_txn().unwrap();
    let err_s = sc
        .insert_many_txn(&mut sw, &duplicate_in_batch)
        .unwrap_err();
    drop(sw);
    let mut cw = cs.write_txn().unwrap();
    let err_c = cc
        .insert_many_txn(&mut cw, &duplicate_in_batch)
        .unwrap_err();
    drop(cw);
    assert_eq!(err_s.to_string(), err_c.to_string());
    assert!(sc.find_by_key("twice").unwrap().is_none());
    assert!(cc.find_by_key("twice").unwrap().is_none());
}

#[test]
fn unique_failures_leave_no_partial_state_when_the_transaction_commits() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let c = store
        .create_columnar_collection("docs", &nonodb_core::columnar::Settings::default())
        .unwrap();
    c.create_index("v", true).unwrap();
    c.insert_one(json!({"_id": "i0", "v": 1})).unwrap();

    // The daemon commits the shared transaction even when one job returns an
    // error, so each failed write must have made no changes before returning.
    let mut wtxn = store.write_txn().unwrap();
    let err = c
        .insert_one_txn(&mut wtxn, json!({"_id": "x", "v": 1}))
        .unwrap_err();
    assert!(err.to_string().contains("duplicate key"), "{err}");
    wtxn.commit().unwrap();
    assert!(c.find_by_key("x").unwrap().is_none());
    assert_eq!(c.count(&json!({"v": 1})).unwrap(), 1);

    c.insert_one(json!({"_id": "i1", "v": 2})).unwrap();
    let mut wtxn = store.write_txn().unwrap();
    let err = c
        .update_txn(
            &mut wtxn,
            &json!({"_id": "i1"}),
            &json!({"$set": {"v": 1}}),
            false,
        )
        .unwrap_err();
    assert!(err.to_string().contains("duplicate key"), "{err}");
    wtxn.commit().unwrap();
    assert_eq!(c.find_by_key("i1").unwrap().unwrap()["v"], 2);
    assert_eq!(c.count(&json!({"v": 1})).unwrap(), 1);

    let batch = [json!({"_id": "n1", "v": 3}), json!({"_id": "n2", "v": 1})];
    let mut wtxn = store.write_txn().unwrap();
    let err = c.insert_many_txn(&mut wtxn, &batch).unwrap_err();
    assert!(err.to_string().contains("duplicate key"), "{err}");
    wtxn.commit().unwrap();
    assert!(c.find_by_key("n1").unwrap().is_none());
    assert!(c.find_by_key("n2").unwrap().is_none());
    assert_eq!(c.count(&json!({"v": 1})).unwrap(), 1);

    let opts = nonodb_core::FindAndModifyOptions {
        filter: &json!({"_id": "i1"}),
        update: Some(&json!({"$set": {"v": 1}})),
        sort: &[],
        new: false,
        upsert: false,
        remove: false,
        array_filters: &[],
        projection: None,
    };
    let mut wtxn = store.write_txn().unwrap();
    let err = c.find_and_modify_txn(&mut wtxn, &opts).unwrap_err();
    assert!(err.to_string().contains("duplicate key"), "{err}");
    wtxn.commit().unwrap();
    assert_eq!(c.find_by_key("i1").unwrap().unwrap()["v"], 2);
    assert_eq!(c.count(&json!({"v": 1})).unwrap(), 1);
}
