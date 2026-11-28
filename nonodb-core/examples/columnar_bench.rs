//! Raw standard-vs-columnar benchmark for the release performance gates.
//!
//! Run with: `cargo run --release -p nonodb-core --example columnar_bench`
//! Results are printed to stdout; they are not committed to the repository.

use std::time::{Duration, Instant};

use nonodb_core::columnar::Settings;
use nonodb_core::{CollectionProfile, FindQuery, Store};
use serde_json::{json, Value};

const DOCS: usize = 100_000;
const BATCH: usize = 1_000;
const POINT_READS: usize = 10_000;
const UPDATES: usize = 2_000;

fn doc(index: usize) -> Value {
    json!({
        "_id": format!("id-{index:08}"),
        "ts": 1_700_000_000_000i64 + index as i64,
        "user": format!("user-{}", index % 10_000),
        "region": (["eu", "us", "apac"][index % 3]),
        "status": (["new", "paid", "shipped", "closed"][index % 4]),
        "amount": (index as f64) / 7.0,
        "qty": index % 97,
        "flag": index.is_multiple_of(2),
        "tags": [index % 5, (index + 1) % 5],
        "nested": {"a": index % 1000, "b": format!("txt-{}", index % 500)},
    })
}

fn id(index: usize) -> String {
    format!("id-{index:08}")
}

fn pct(values: &mut [Duration], p: f64) -> Duration {
    values.sort_unstable();
    let index = ((values.len() as f64 - 1.0) * p).round() as usize;
    values[index]
}

fn insert_batches(coll: &nonodb_core::Collection<'_>, store: &Store) {
    for start in (0..DOCS).step_by(BATCH) {
        let docs: Vec<Value> = (start..(start + BATCH).min(DOCS)).map(doc).collect();
        let mut wtxn = store.write_txn().unwrap();
        let tx_coll = store.collection_in(&mut wtxn, coll.name()).unwrap();
        tx_coll.insert_many_txn(&mut wtxn, &docs).unwrap();
        wtxn.commit().unwrap();
    }
}

struct Run {
    label: &'static str,
    coll: nonodb_core::Collection<'static>,
    _store: &'static Store,
    _dir: tempfile::TempDir,
    insert_batch: Duration,
    single_insert: Duration,
    point_p50: Duration,
    point_p95: Duration,
    open_point_p50: Duration,
    open_point_p95: Duration,
    scan: Duration,
    aggregate: Duration,
    update_p50: Duration,
    multi_update: Duration,
    multi_update_rows: usize,
    storage: u64,
    after_delete: u64,
    after_compact: u64,
    seal_p95: Duration,
}

fn run(label: &'static str, profile: CollectionProfile) -> Run {
    let dir = tempfile::tempdir().unwrap();
    let store: &'static Store = Box::leak(Box::new(Store::open(dir.path()).unwrap()));
    let coll: nonodb_core::Collection<'static> = match profile {
        CollectionProfile::Standard => store.create_collection("docs", profile).unwrap(),
        CollectionProfile::Columnar => store
            .create_columnar_collection(
                "docs",
                &Settings {
                    bucket_max_docs: 512,
                    ..Default::default()
                },
            )
            .unwrap(),
        _ => unreachable!(),
    };

    let start = Instant::now();
    insert_batches(&coll, store);
    let insert_batch = start.elapsed();

    // The final staging bucket is still open: measure point reads there before
    // the explicit seal.
    let mut open_point = Vec::with_capacity(POINT_READS.min(512));
    let open_reads = POINT_READS.min(512);
    for index in 0..open_reads {
        let id = id(DOCS - 1 - (index % 128));
        let start = Instant::now();
        let found = coll.find_by_key(&id).unwrap();
        open_point.push(start.elapsed());
        assert!(found.is_some());
    }
    let open_point_p50 = pct(&mut open_point, 0.50);
    let open_point_p95 = pct(&mut open_point, 0.95);

    store.seal_columnar("docs").unwrap();

    let single_start = Instant::now();
    for index in 0..1_000 {
        let mut d = doc(index + DOCS);
        d["_id"] = json!(format!("single-{index:08}"));
        coll.insert_one(d).unwrap();
    }
    let single_insert = single_start.elapsed() / 1_000;

    let mut point = Vec::with_capacity(POINT_READS);
    for index in 0..POINT_READS {
        let start = Instant::now();
        let found = coll.find_by_key(&id(index % (DOCS - 1))).unwrap();
        point.push(start.elapsed());
        assert!(found.is_some());
    }

    let scan_start = Instant::now();
    let rows = coll
        .find(&FindQuery {
            filter: json!({"flag": true}),
            ..Default::default()
        })
        .unwrap();
    let scan = scan_start.elapsed();
    assert!(!rows.is_empty());

    let agg_start = Instant::now();
    let groups = coll
        .aggregate(&[
            json!({"$group": {"_id": "$status", "n": {"$sum": 1}, "total": {"$sum": "$amount"}}}),
            json!({"$sort": {"_id": 1}}),
        ])
        .unwrap();
    let aggregate = agg_start.elapsed();
    assert_eq!(groups.len(), 4);

    let mut updates = Vec::with_capacity(UPDATES);
    for index in 0..UPDATES {
        let start = Instant::now();
        let changed = coll
            .update(
                &json!({"_id": id(index * 3)}),
                &json!({"$inc": {"qty": 1}}),
                false,
            )
            .unwrap();
        updates.push(start.elapsed());
        assert_eq!(changed, 1);
    }

    coll.create_index("user", false).unwrap();
    coll.create_index("status", false).unwrap();
    store.seal_columnar("docs").unwrap();
    store.compact_columnar("docs").unwrap();
    let storage_path = dir.path().join("compact-size.mdb");
    store
        .copy_to_file(&storage_path, nonodb_core::heed::CompactionOption::Enabled)
        .unwrap();
    let storage = std::fs::metadata(&storage_path).unwrap().len();
    if profile == CollectionProfile::Columnar {
        let stats = store.columnar_stats("docs").unwrap().unwrap();
        println!(
            "columnar storage parts rows={} buckets={} open={} sealed={} id_entries={} deletes={} data={} open_bytes={} sealed_bytes={} id_bytes={}",
            stats.rows,
            stats.buckets,
            stats.open_buckets,
            stats.sealed_buckets,
            stats.id_entries,
            stats.deletes,
            stats.data_bytes,
            stats.open_bytes,
            stats.sealed_bytes,
            stats.id_bytes
        );
    }

    let deleted = coll.delete(&json!({"qty": {"$lt": 25}}), true).unwrap();
    assert!(deleted > 0);
    let after_delete_path = dir.path().join("after-delete.mdb");
    store
        .copy_to_file(
            &after_delete_path,
            nonodb_core::heed::CompactionOption::Enabled,
        )
        .unwrap();
    let after_delete = std::fs::metadata(&after_delete_path).unwrap().len();
    if profile == CollectionProfile::Columnar {
        store.compact_columnar("docs").unwrap();
    }
    let after_compact = if profile == CollectionProfile::Columnar {
        let path = dir.path().join("after-compact.mdb");
        store
            .copy_to_file(&path, nonodb_core::heed::CompactionOption::Enabled)
            .unwrap();
        std::fs::metadata(&path).unwrap().len()
    } else {
        after_delete
    };

    let mut seals = Vec::new();
    if profile == CollectionProfile::Columnar {
        let settings = store.columnar_settings("docs").unwrap().unwrap();
        for _ in 0..50 {
            let start = Instant::now();
            let mut wtxn = store.write_txn().unwrap();
            nonodb_core::columnar::compact(&mut wtxn, store.env(), "docs", &settings).unwrap();
            wtxn.commit().unwrap();
            seals.push(start.elapsed());
        }
    }

    // Multi-row update runs after the storage snapshot so it cannot perturb
    // the retention/compaction gate.
    let multi_start = Instant::now();
    let multi_update_rows = coll
        .update(&json!({"flag": true}), &json!({"$inc": {"qty": 1}}), true)
        .unwrap();
    let multi_update = multi_start.elapsed();

    Run {
        label,
        coll,
        _store: store,
        _dir: dir,
        insert_batch,
        single_insert,
        point_p50: pct(&mut point, 0.50),
        point_p95: pct(&mut point, 0.95),
        open_point_p50,
        open_point_p95,
        scan,
        aggregate,
        update_p50: pct(&mut updates, 0.50),
        multi_update,
        multi_update_rows,
        storage,
        after_delete,
        after_compact,
        seal_p95: if seals.is_empty() {
            Duration::ZERO
        } else {
            pct(&mut seals, 0.95)
        },
    }
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() {
    println!(
        "docs={DOCS} point_reads={POINT_READS} updates={UPDATES} batch={BATCH} bucket_max_docs=512"
    );
    let standard = run("standard", CollectionProfile::Standard);
    let columnar = run("columnar", CollectionProfile::Columnar);

    println!(
        "{:<10} {:>14} {:>14} {:>10} {:>14} {:>14} {:>14} {:>14} {:>14} {:>14} {:>14}",
        "profile",
        "batch_ms",
        "single_us",
        "point_us",
        "scan_ms",
        "agg_ms",
        "update_us",
        "open_point_us",
        "multi_update_ms",
        "multi_update_rows",
        "storage_mb"
    );
    for run in [&standard, &columnar] {
        println!(
            "{:<10} {:>14.1} {:>14.2} {:>10.1} {:>14.1} {:>14.1} {:>14.1} {:>14.1} {:>14.1} {:>14.1} {:>14.1}",
            run.label,
            run.insert_batch.as_secs_f64() * 1000.0,
            run.single_insert.as_secs_f64() * 1_000_000.0,
            run.point_p50.as_secs_f64() * 1_000_000.0,
            run.scan.as_secs_f64() * 1000.0,
            run.aggregate.as_secs_f64() * 1000.0,
            run.update_p50.as_secs_f64() * 1_000_000.0,
            run.open_point_p50.as_secs_f64() * 1_000_000.0,
            run.multi_update.as_secs_f64() * 1000.0,
            run.multi_update_rows as f64,
            mb(run.storage),
        );
    }
    println!(
        "storage_ratio={:.3} batch_ratio={:.3} point_ratio={:.3} update_ratio={:.3} scan_ratio={:.3} agg_ratio={:.3}",
        columnar.storage as f64 / standard.storage as f64,
        standard.insert_batch.as_secs_f64() / columnar.insert_batch.as_secs_f64(),
        columnar.point_p50.as_secs_f64() / standard.point_p50.as_secs_f64(),
        columnar.update_p50.as_secs_f64() / standard.update_p50.as_secs_f64(),
        standard.scan.as_secs_f64() / columnar.scan.as_secs_f64(),
        standard.aggregate.as_secs_f64() / columnar.aggregate.as_secs_f64(),
    );
    println!(
        "columnar after_delete_mb={:.1} after_compact_mb={:.1} compact_reclaim_ratio={:.3} seal_compact_p95_ms={:.1} point_p95_us={:.1} open_point_p95_us={:.1}",
        mb(columnar.after_delete),
        mb(columnar.after_compact),
        columnar.after_compact as f64 / columnar.after_delete.max(1) as f64,
        columnar.seal_p95.as_secs_f64() * 1000.0,
        columnar.point_p95.as_secs_f64() * 1_000_000.0,
        columnar.open_point_p95.as_secs_f64() * 1_000_000.0,
    );
    println!(
        "columnar_rows={} standard_rows={}",
        columnar.coll.count(&json!({})).unwrap(),
        standard.coll.count(&json!({})).unwrap()
    );
    drop(columnar);
    drop(standard);
}
