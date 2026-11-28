//! Deterministic randomized differential test for the columnar profile.
//!
//! The generator is a small LCG so every run is reproducible. Each operation
//! is applied to both a standard and a columnar collection, then the complete
//! visible state is compared.

use nonodb_core::{Collection, CollectionProfile, FindQuery, Store};
use serde_json::{json, Value};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 11
    }

    fn pick(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn snapshot(c: &Collection<'_>) -> Vec<Value> {
    c.find(&FindQuery {
        filter: json!({}),
        projection: None,
        sort: vec![("_id".into(), 1)],
        skip: 0,
        limit: None,
        comment: None,
        max_time_ms: None,
        hint: None,
    })
    .unwrap()
}

fn make_doc(id: String, rng: &mut Rng) -> Value {
    let n = rng.pick(1000) as i64;
    let even = n % 2 == 0;
    json!({
        "_id": id,
        "n": n,
        "f": (n as f64) / 7.0,
        "s": (["alpha", "beta", "gamma"][rng.pick(3) as usize]),
        "flag": even,
        "arr": [n % 5, (n + 1) % 5],
        "nested": {
            "x": n % 11,
            "y": if even { Value::Null } else { json!(format!("y{n}")) },
        },
        "maybe": if rng.pick(2) == 0 { Value::Null } else { json!(n % 3) },
    })
}

#[test]
fn randomized_differential_operations_match_standard() {
    let sd = tempfile::tempdir().unwrap();
    let cd = tempfile::tempdir().unwrap();
    let ss = Store::open(sd.path()).unwrap();
    let cs = Store::open(cd.path()).unwrap();
    let sc = ss
        .create_collection("docs", CollectionProfile::Standard)
        .unwrap();
    let cc = cs
        .create_columnar_collection(
            "docs",
            &nonodb_core::columnar::Settings {
                bucket_max_docs: 7,
                ..Default::default()
            },
        )
        .unwrap();

    let mut rng = Rng(0x5eed_c01a_7e57_1234);
    let mut live: Vec<String> = Vec::new();
    for step in 0..400 {
        let op = rng.pick(10);
        match op {
            0..=3 => {
                let id = format!("d{step:04}");
                let doc = make_doc(id.clone(), &mut rng);

                assert_eq!(
                    sc.insert_one(doc.clone()).unwrap(),
                    cc.insert_one(doc).unwrap()
                );
                live.push(id);
            }
            4..=5 if !live.is_empty() => {
                let id = live[rng.pick(live.len() as u64) as usize].clone();
                let update = match rng.pick(4) {
                    0 => json!({"$inc": {"n": 1}}),
                    1 => json!({"$set": {"s": "beta", "nested.x": 9}}),
                    2 => json!({"$push": {"arr": rng.pick(10) as i64}}),
                    _ => json!({"$unset": {"nested.y": ""}, "$set": {"maybe": null}}),
                };
                let filter = json!({"_id": id});
                assert_eq!(
                    sc.update(&filter, &update, false).unwrap(),
                    cc.update(&filter, &update, false).unwrap(),
                    "update step {step} {update}"
                );
            }
            6 if !live.is_empty() => {
                let index = rng.pick(live.len() as u64) as usize;
                let id = live.remove(index);
                let filter = json!({"_id": id});
                assert_eq!(
                    sc.delete(&filter, false).unwrap(),
                    cc.delete(&filter, false).unwrap()
                );
            }
            7 => {
                let id = format!("upsert-{step}");
                let filter = json!({"_id": id});
                let update = json!({"$set": {"n": step, "s": "upsert"}});
                let sc_result = sc.update_opts(&filter, &update, false, true).unwrap();
                let cc_result = cc.update_opts(&filter, &update, false, true).unwrap();
                assert_eq!(sc_result.upserted_id, cc_result.upserted_id);
                if sc_result.upserted_id.is_some() {
                    live.push(id);
                }
            }
            8 => {
                let filter = match rng.pick(4) {
                    0 => json!({"flag": true}),
                    1 => json!({"n": {"$gte": 100}}),
                    2 => json!({"s": {"$in": ["alpha", "gamma"]}}),
                    _ => json!({"arr": 2}),
                };
                let sort = vec![("n".into(), if rng.pick(2) == 0 { 1 } else { -1 })];
                let query = FindQuery {
                    filter: filter.clone(),
                    projection: Some(json!({"nested": 0})),
                    sort,
                    skip: rng.pick(3) as usize,
                    limit: Some(1 + rng.pick(5) as usize),
                    comment: None,
                    max_time_ms: None,
                    hint: None,
                };
                assert_eq!(sc.find(&query).unwrap(), cc.find(&query).unwrap());
                assert_eq!(
                    sc.count(&filter).unwrap(),
                    cc.count(&filter).unwrap(),
                    "count step {step}"
                );
            }
            _ => {}
        }
        if step % 20 == 0 {
            assert_eq!(snapshot(&sc), snapshot(&cc), "snapshot step {step}");
        }
    }
    assert_eq!(snapshot(&sc), snapshot(&cc));

    // Indexes and compaction must not change the visible state.
    for c in [&sc, &cc] {
        c.create_index("n", false).unwrap();
        c.create_index("s", false).unwrap();
    }
    assert_eq!(snapshot(&sc), snapshot(&cc));
    cs.compact_columnar("docs").unwrap();
    assert_eq!(snapshot(&sc), snapshot(&cc));
}
