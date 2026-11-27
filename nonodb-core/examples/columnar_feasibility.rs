//! Phase 0 feasibility benchmark for a general-purpose columnar collection.
//!
//! This intentionally uses the existing time-series profile as a proxy for the
//! proposed columnar storage. It measures:
//!
//! - generic document shapes with realistic user-supplied UUID-sized `_id`s;
//! - the cost of the user-id column and the `0x05` id locator map;
//! - uncompacted and compacted sizes against a standard collection;
//! - point reads, updates, and deletes by `_id`.
//!
//! No production API is added here. Run with:
//!
//! ```text
//! ROWS=100000 SERIES=16 READS=500 \
//!   cargo run --release -p nonodb-core --example columnar_feasibility
//! ```

use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use nonodb_core::heed::CompactionOption;
use nonodb_core::{Collection, Store};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() - 1) as f64 * pct).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn uuid_like(idx: usize) -> String {
    // 36 characters, matching the common UUID string length without needing a
    // random source. The values remain deterministic for reproduction.
    format!(
        "{:08x}-{:04x}-4{:03x}-a{:03x}-{:012x}",
        idx as u32,
        (idx * 17) as u16,
        (idx * 29) as u16 & 0x0fff,
        (idx * 43) as u16 & 0x0fff,
        (idx as u64 * 0x9e37_79b9) & 0xffff_ffff_ffff
    )
}

fn generic_doc(series: usize, idx: usize, with_id: bool) -> Value {
    let ts = 1_700_000_000_000i64 + (idx as i64 / 10) * 1000;
    let status = ["ok", "warn", "error", "offline"][idx % 4];
    let kind = ["reading", "alert", "audit", "checkpoint"][idx % 4];
    let region = ["eu-west", "eu-central", "us-east"][(idx / 1000) % 3];
    let mut doc = json!({
        "ts": {"$date": {"$numberLong": ts.to_string()}},
        "device": {
            "id": format!("device-{series}"),
            "site": format!("site-{}", series % 4),
            "region": region,
        },
        "kind": kind,
        "status": status,
        "temperature": 21.0 + (idx % 97) as f64 * 0.01,
        "humidity": 40.0 + (idx % 53) as f64 * 0.05,
        "counter": (idx % 10_000) as i64,
        "ok": !idx.is_multiple_of(3),
        "tags": [format!("tag-{}", idx % 7), "generic", "phase0"],
        "nested": {
            "batch": (idx / 500) as i64,
            "shard": (idx % 5) as i64,
            "note": if idx.is_multiple_of(13) { "burst" } else { "steady" },
        },
    });
    if idx.is_multiple_of(7) {
        doc.as_object_mut()
            .expect("object")
            .insert("optional_note".into(), json!("schema-drift"));
    }
    if idx.is_multiple_of(11) {
        doc.as_object_mut()
            .expect("object")
            .insert("optional_array".into(), json!([idx, idx + 1, idx + 2]));
    }
    if with_id {
        doc.as_object_mut()
            .expect("object")
            .insert("_id".into(), json!(uuid_like(idx)));
    }
    doc
}

fn compacted_bytes(
    store: &Store,
    dir: &Path,
    name: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let path = dir.join(format!("{name}-compacted.mdb"));
    let file = store.copy_to_file(&path, CompactionOption::Enabled)?;
    file.sync_all()?;
    Ok(std::fs::metadata(path)?.len())
}

struct ResultRow {
    name: &'static str,
    rows: usize,
    raw_bytes: u64,
    uncompacted: u64,
    compacted: u64,
    insert: Duration,
    point: Duration,
    update: Duration,
    delete: Duration,
}

impl ResultRow {
    fn print(&self) {
        println!(
            "{:<24} rows={:>7} raw={:>7.2} MB ({:>7.2} B/row) uncompacted={:>7.2} MB ({:>7.2} B/row) compacted={:>7.2} MB ({:>7.2} B/row)",
            self.name,
            self.rows,
            self.raw_bytes as f64 / 1_048_576.0,
            self.raw_bytes as f64 / self.rows as f64,
            self.uncompacted as f64 / 1_048_576.0,
            self.uncompacted as f64 / self.rows as f64,
            self.compacted as f64 / 1_048_576.0,
            self.compacted as f64 / self.rows as f64,
        );
        println!(
            "  insert={:?} ({:.0} rows/s) point={:?} update={:?} delete={:?}",
            self.insert,
            self.rows as f64 / self.insert.as_secs_f64(),
            self.point,
            self.update,
            self.delete,
        );
    }
}

fn run_ts(
    dir: &Path,
    name: &'static str,
    rows: usize,
    series: usize,
    reads: usize,
    with_ids: bool,
) -> Result<ResultRow, Box<dyn std::error::Error>> {
    let store = Store::open(dir)?;
    let settings = nonodb_core::timeseries::Settings {
        time_field: "ts".into(),
        meta_field: Some("device".into()),
        bucket_max_docs: env_usize("BUCKET_DOCS", 2048),
        ..Default::default()
    };
    let collection: Collection = store.create_timeseries_collection("bench", &settings)?;

    let started = Instant::now();
    let mut ids = Vec::with_capacity(rows);
    let mut raw_bytes = 0u64;
    for idx in 0..rows {
        let doc = generic_doc(idx % series, idx, with_ids);
        raw_bytes += serde_json::to_vec(&doc)?.len() as u64;
        let id = collection.insert_one(doc)?;
        ids.push(id);
    }
    let insert = started.elapsed();
    let stats_before = collection.timeseries_stats()?.unwrap_or_default();
    let uncompacted = std::fs::metadata(dir.join("data.mdb"))?.len();
    store.seal_timeseries("bench")?;
    let stats = collection.timeseries_stats()?.unwrap_or_default();
    let compacted = compacted_bytes(&store, dir, "sealed")?;

    let sample = reads.min(rows);
    let step = (rows / sample.max(1)).max(1);
    let mut point_samples = Vec::with_capacity(sample);
    for idx in (0..rows).step_by(step).take(sample) {
        let op = Instant::now();
        let doc = collection.find_by_key(&ids[idx])?;
        assert!(doc.is_some(), "point read missed {idx}");
        point_samples.push(op.elapsed());
    }

    let update_n = (reads / 4).clamp(1, 100).min(rows);
    let mut update_samples = Vec::with_capacity(update_n);
    for idx in (0..rows).step_by(step).take(update_n) {
        let op = Instant::now();
        let changed = collection.update(
            &json!({"_id": ids[idx]}),
            &json!({"$set": {"status": "updated", "counter": -1}}),
            false,
        )?;
        assert_eq!(changed, 1, "update missed {idx}");
        update_samples.push(op.elapsed());
    }

    let delete_n = (reads / 4).clamp(1, 100).min(rows);
    let mut delete_samples = Vec::with_capacity(delete_n);
    for idx in (0..rows).step_by(step).skip(update_n).take(delete_n) {
        let op = Instant::now();
        let deleted = collection.delete(&json!({"_id": ids[idx]}), false)?;
        assert_eq!(deleted, 1, "delete missed {idx}");
        delete_samples.push(op.elapsed());
    }

    println!(
        "  ts {}: before_seal open={} sealed={} raw={} encoded={} open_bytes={}",
        name,
        stats_before.open_buckets,
        stats_before.sealed_buckets,
        stats_before.raw_bytes,
        stats_before.encoded_bytes,
        stats_before.open_bytes
    );
    println!(
        "  ts {} after_seal: open={} sealed={} rows={} encoded_bytes={} compressed={:.2}x",
        name,
        stats.open_buckets,
        stats.sealed_buckets,
        stats.rows,
        stats.encoded_bytes,
        stats.raw_bytes as f64 / stats.encoded_bytes.max(1) as f64
    );
    Ok(ResultRow {
        name,
        rows,
        raw_bytes,
        uncompacted,
        compacted,
        insert,
        point: percentile(
            &{
                let mut sorted = point_samples.clone();
                sorted.sort();
                sorted
            },
            0.50,
        ),
        update: percentile(
            &{
                let mut sorted = update_samples.clone();
                sorted.sort();
                sorted
            },
            0.50,
        ),
        delete: percentile(
            &{
                let mut sorted = delete_samples.clone();
                sorted.sort();
                sorted
            },
            0.50,
        ),
    })
}

fn run_standard(
    dir: &Path,
    name: &'static str,
    rows: usize,
    series: usize,
    reads: usize,
) -> Result<ResultRow, Box<dyn std::error::Error>> {
    let store = Store::open(dir)?;
    let collection = store.create_collection("bench", nonodb_core::CollectionProfile::Standard)?;
    let started = Instant::now();
    let mut ids = Vec::with_capacity(rows);
    let mut raw_bytes = 0u64;
    for idx in 0..rows {
        let doc = generic_doc(idx % series, idx, true);
        raw_bytes += serde_json::to_vec(&doc)?.len() as u64;
        ids.push(collection.insert_one(doc)?);
    }
    let insert = started.elapsed();
    let uncompacted = std::fs::metadata(dir.join("data.mdb"))?.len();
    let compacted = compacted_bytes(&store, dir, "standard")?;

    let sample = reads.min(rows);
    let step = (rows / sample.max(1)).max(1);
    let mut point_samples = Vec::with_capacity(sample);
    for idx in (0..rows).step_by(step).take(sample) {
        let op = Instant::now();
        assert!(collection.find_by_key(&ids[idx])?.is_some());
        point_samples.push(op.elapsed());
    }

    let update_n = (reads / 4).clamp(1, 100).min(rows);
    let mut update_samples = Vec::with_capacity(update_n);
    for idx in (0..rows).step_by(step).take(update_n) {
        let op = Instant::now();
        let changed = collection.update(
            &json!({"_id": ids[idx]}),
            &json!({"$set": {"status": "updated", "counter": -1}}),
            false,
        )?;
        assert_eq!(changed, 1);
        update_samples.push(op.elapsed());
    }

    let delete_n = (reads / 4).clamp(1, 100).min(rows);
    let mut delete_samples = Vec::with_capacity(delete_n);
    for idx in (0..rows).step_by(step).skip(update_n).take(delete_n) {
        let op = Instant::now();
        let deleted = collection.delete(&json!({"_id": ids[idx]}), false)?;
        assert_eq!(deleted, 1);
        delete_samples.push(op.elapsed());
    }

    Ok(ResultRow {
        name,
        rows,
        raw_bytes,
        uncompacted,
        compacted,
        insert,
        point: percentile(
            &{
                let mut sorted = point_samples;
                sorted.sort();
                sorted
            },
            0.50,
        ),
        update: percentile(
            &{
                let mut sorted = update_samples;
                sorted.sort();
                sorted
            },
            0.50,
        ),
        delete: percentile(
            &{
                let mut sorted = delete_samples;
                sorted.sort();
                sorted
            },
            0.50,
        ),
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows = env_usize("ROWS", 100_000);
    let series = env_usize("SERIES", 16);
    let reads = env_usize("READS", 500);
    let bucket_docs = env_usize("BUCKET_DOCS", 2048);
    println!(
        "columnar feasibility (TS proxy): rows={rows} series={series} reads={reads} bucket_docs={bucket_docs}\n"
    );

    let standard_dir = tempfile::tempdir()?;
    let standard = run_standard(standard_dir.path(), "standard+ids", rows, series, reads)?;

    let ts_ids_dir = tempfile::tempdir()?;
    let ts_ids = run_ts(ts_ids_dir.path(), "ts+user_ids", rows, series, reads, true)?;

    let ts_synth_dir = tempfile::tempdir()?;
    let ts_synth = run_ts(
        ts_synth_dir.path(),
        "ts+synthetic_ids",
        rows,
        series,
        reads,
        false,
    )?;

    println!("\nsummary");
    for row in [&standard, &ts_ids, &ts_synth] {
        row.print();
    }
    let user_id_cost = ts_ids.compacted.saturating_sub(ts_synth.compacted);
    println!(
        "\nuser-id cost: compacted {} bytes ({:.2} B/row) = {:.2} MB",
        user_id_cost,
        user_id_cost as f64 / rows as f64,
        user_id_cost as f64 / 1_048_576.0
    );
    println!(
        "storage vs standard: ts+user_ids {:.2}x smaller, synthetic {:.2}x smaller",
        standard.compacted as f64 / ts_ids.compacted as f64,
        standard.compacted as f64 / ts_synth.compacted as f64
    );
    println!(
        "point read: ts+user_ids {:.2}x standard; update: {:.2}x; delete: {:.2}x",
        ts_ids.point.as_secs_f64() / standard.point.as_secs_f64().max(1e-12),
        ts_ids.update.as_secs_f64() / standard.update.as_secs_f64().max(1e-12),
        ts_ids.delete.as_secs_f64() / standard.delete.as_secs_f64().max(1e-12)
    );
    Ok(())
}
