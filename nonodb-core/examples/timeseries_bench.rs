//! Write/read benchmark for time-series collections.
//!
//! Generates a fixed-rate IoT workload across several series and measures:
//!
//! - insert throughput and per-insert latency percentiles;
//! - bucket sealing cost;
//! - point lookup, narrow and wide time-range reads, match-all scans, counts;
//! - secondary-index create/lookup, `$set` updates, SQL `GROUP BY`, and the
//!   deletion bitmap with a compacted copy;
//! - the same operations against a standard (document-per-row) collection as a
//!   baseline.
//!
//! Run with:
//!
//! ```text
//! cargo run --release -p nonodb-core --example timeseries_bench
//! ```
//!
//! Environment: `ROWS` (default 100000), `SERIES` (default 10),
//! `READS` (default 2000).

use std::time::{Duration, Instant};

use serde_json::{json, Value};

use nonodb_core::{FindQuery, Store};

fn query(filter: Value, limit: Option<usize>) -> FindQuery {
    FindQuery {
        filter,
        projection: None,
        sort: Vec::new(),
        skip: 0,
        limit,
        comment: None,
        max_time_ms: None,
        hint: None,
    }
}

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() - 1) as f64 * pct).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn report_latency(label: &str, mut samples: Vec<Duration>, ops: usize) {
    samples.sort();
    let total: Duration = samples.iter().sum();
    println!(
        "{label:<34} {ops:>7} ops  {:>9.1} ops/s  p50 {:>8.3?}  p95 {:>8.3?}  p99 {:>8.3?}",
        ops as f64 / total.as_secs_f64(),
        percentile(&samples, 0.50),
        percentile(&samples, 0.95),
        percentile(&samples, 0.99),
    );
}

fn doc(series: usize, idx: usize) -> Value {
    let ts = 1_700_000_000_000i64 + (idx as i64 / 10) * 1000;
    let status = ["ok", "warn", "error", "offline"][idx % 4];
    json!({
        "ts": {"$date": {"$numberLong": ts.to_string()}},
        "device": {"id": format!("sensor-{series}"), "site": "plant-1"},
        "temperature": 21.0 + (idx % 97) as f64 * 0.01,
        "humidity": 40.0 + (idx % 53) as f64 * 0.05,
        "pressure": 1013.0 + (idx % 31) as f64 * 0.1,
        "status": status,
        "level": (idx % 8) as i64,
        "ok": !idx.is_multiple_of(3),
    })
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows = env_usize("ROWS", 100_000);
    let series = env_usize("SERIES", 10);
    let reads = env_usize("READS", 2_000);

    println!("nonodb time-series benchmark: rows={rows} series={series} reads={reads}\n");

    // ------------------------------------------------------------------
    // Time-series collection
    // ------------------------------------------------------------------
    let dir = tempfile::tempdir()?;
    let store = Store::open(dir.path())?;
    let settings = nonodb_core::timeseries::Settings {
        time_field: "ts".into(),
        meta_field: Some("device".into()),
        ..Default::default()
    };
    let ts = store.create_timeseries_collection("tsbench", &settings)?;

    let started = Instant::now();
    let mut insert_latency = Vec::with_capacity(rows);
    for idx in 0..rows {
        let op = Instant::now();
        ts.insert_one(doc(idx % series, idx))?;
        insert_latency.push(op.elapsed());
    }
    let insert_total = started.elapsed();

    let stats_open = ts.timeseries_stats()?.unwrap_or_default();
    let seal_started = Instant::now();
    store.seal_timeseries("tsbench")?;
    let seal_total = seal_started.elapsed();
    let stats = ts.timeseries_stats()?.unwrap_or_default();

    println!("write");
    report_latency("insert (time-series)", insert_latency, rows);
    println!(
        "{:<34} {:>7}    {:>9.0} rows/s (wall)",
        "insert wall clock",
        rows,
        rows as f64 / insert_total.as_secs_f64()
    );
    println!(
        "{:<34} {:>7}    {:>9.2?} total, {:.2?}/bucket",
        "seal open buckets",
        stats_open.open_buckets,
        seal_total,
        if stats_open.open_buckets == 0 {
            Duration::ZERO
        } else {
            seal_total / stats_open.open_buckets as u32
        }
    );
    println!(
        "stored: {} bytes for {} rows = {:.2} bytes/row ({:.1}x vs raw JSON), buckets={} sealed={}",
        stats.total_bytes(),
        stats.rows,
        stats.total_bytes() as f64 / stats.rows.max(1) as f64,
        stats.raw_bytes as f64 / stats.total_bytes().max(1) as f64,
        stats.buckets,
        stats.sealed_buckets,
    );

    // ------------------------------------------------------------------
    // Time-series reads
    // ------------------------------------------------------------------
    println!("\nread (time-series collection)");
    let base = 1_700_000_000_000i64;

    // Point lookup by synthetic _id (full bucket scan, no index).
    let sample = ts
        .find(&query(json!({}), Some(1)))?
        .into_iter()
        .next()
        .ok_or("no sample")?;
    let sample_id = sample["_id"].as_str().ok_or("no id")?.to_string();
    let mut samples = Vec::with_capacity(reads);
    for _ in 0..reads {
        let op = Instant::now();
        let doc = ts.find_by_key(&sample_id)?;
        assert!(doc.is_some());
        samples.push(op.elapsed());
    }
    report_latency("find_by_key (point, no index)", samples, reads);

    // One-minute window for a single series: ~6 rows, prunes almost all buckets.
    let narrow = json!({
        "ts": {"$gte": {"$date": {"$numberLong": (base + 3_600_000).to_string()}},
               "$lt":  {"$date": {"$numberLong": (base + 3_660_000).to_string()}}},
        "device.id": "sensor-0"
    });
    let mut samples = Vec::with_capacity(reads);
    let mut narrow_rows = 0usize;
    for _ in 0..reads {
        let op = Instant::now();
        let docs = ts.find(&query(narrow.clone(), None))?;
        narrow_rows = docs.len();
        samples.push(op.elapsed());
    }
    report_latency(
        &format!("find 1 min, 1 series ({narrow_rows} rows)"),
        samples,
        reads,
    );

    // One-hour window for a single series: ~360 rows.
    let wide = json!({
        "ts": {"$gte": {"$date": {"$numberLong": base.to_string()}},
               "$lt":  {"$date": {"$numberLong": (base + 3_600_000).to_string()}}},
        "device.id": "sensor-0"
    });
    let mut samples = Vec::with_capacity(reads / 4);
    let mut wide_rows = 0usize;
    for _ in 0..(reads / 4).max(1) {
        let op = Instant::now();
        let docs = ts.find(&query(wide.clone(), None))?;
        wide_rows = docs.len();
        samples.push(op.elapsed());
    }
    report_latency(
        &format!("find 1 h, 1 series ({wide_rows} rows)"),
        samples,
        (reads / 4).max(1),
    );

    // Count over a series and over everything.
    let mut samples = Vec::with_capacity(reads / 4);
    for _ in 0..(reads / 4).max(1) {
        let op = Instant::now();
        ts.count(&wide)?;
        samples.push(op.elapsed());
    }
    report_latency("count 1 h, 1 series", samples, (reads / 4).max(1));

    // Downsampling aggregation: one hour of one series grouped by status.
    let pipeline = vec![
        json!({"$match": wide.clone()}),
        json!({"$group": {
            "_id": "$status",
            "samples": {"$sum": 1},
            "avg_temp": {"$avg": "$temperature"}
        }}),
        json!({"$sort": {"_id": 1}}),
    ];
    let agg_runs = (reads / 10).max(1);
    let mut samples = Vec::with_capacity(agg_runs);
    let mut groups = 0usize;
    for _ in 0..agg_runs {
        let op = Instant::now();
        let result = ts.aggregate(&pipeline)?;
        groups = result.len();
        samples.push(op.elapsed());
    }
    report_latency(
        &format!("aggregate $group 1 h, 1 series ({groups} groups)"),
        samples,
        agg_runs,
    );

    // Point deletes by synthetic id: sealed buckets take a tombstone, so the
    // cost is one LMDB put plus the bucket header read.
    let delete_sample = (rows / 2000).clamp(1, 100);
    let doomed: Vec<String> = ts
        .find(&query(json!({}), Some(delete_sample)))?
        .iter()
        .filter_map(|doc| doc["_id"].as_str().map(str::to_string))
        .collect();
    let mut samples = Vec::with_capacity(doomed.len());
    for id in &doomed {
        let op = Instant::now();
        ts.delete(&json!({"_id": id}), false)?;
        samples.push(op.elapsed());
    }
    report_latency("delete by _id (bitmap)", samples, doomed.len());

    let scans = (reads / 50).max(1);
    let mut samples = Vec::with_capacity(scans);
    let mut matched = 0usize;
    for _ in 0..scans {
        let op = Instant::now();
        let docs = ts.find(&query(json!({}), None))?;
        matched = docs.len();
        samples.push(op.elapsed());
    }
    report_latency(&format!("find match-all ({matched} rows)"), samples, scans);

    // Decode only: read the whole collection 1 series at a time.
    let mut samples = Vec::with_capacity(scans);
    for _ in 0..scans {
        let op = Instant::now();
        for series_idx in 0..series {
            ts.find(&query(
                json!({"device.id": format!("sensor-{series_idx}")}),
                None,
            ))?;
        }
        samples.push(op.elapsed());
    }
    report_latency(
        &format!("find all rows series-by-series ({series} queries)"),
        samples,
        scans,
    );

    // ------------------------------------------------------------------
    // Index, update, and SQL
    // ------------------------------------------------------------------
    println!("\nindex, update, and SQL (time-series collection)");
    let index_started = Instant::now();
    ts.create_index("status", false)?;
    println!(
        "{:<34} {:>9.2?}",
        "createIndex status",
        index_started.elapsed()
    );
    let indexed = query(
        json!({"status": "error", "device.id": "sensor-0"}),
        Some(100),
    );
    let mut samples = Vec::with_capacity(reads / 10);
    let mut indexed_rows = 0usize;
    for _ in 0..(reads / 10).max(1) {
        let op = Instant::now();
        indexed_rows = ts.find(&indexed)?.len();
        samples.push(op.elapsed());
    }
    report_latency(
        &format!("find indexed status+series ({indexed_rows} rows)"),
        samples,
        (reads / 10).max(1),
    );
    let plan = ts.explain(&indexed)?;
    println!(
        "{:<34} index_used={} access={}",
        "explain indexed query",
        plan.get("index_used")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        plan.get("access").and_then(Value::as_str).unwrap_or("?")
    );

    let update_sample = (rows / 1000).clamp(1, 100);
    let updatable: Vec<String> = ts
        .find(&query(json!({"status": "ok"}), Some(update_sample)))?
        .iter()
        .filter_map(|doc| doc["_id"].as_str().map(str::to_string))
        .collect();
    let mut samples = Vec::with_capacity(updatable.len());
    for id in &updatable {
        let op = Instant::now();
        ts.update(&json!({"_id": id}), &json!({"$set": {"level": 9}}), false)?;
        samples.push(op.elapsed());
    }
    report_latency(
        &format!("update $set by _id ({} rows)", updatable.len()),
        samples,
        updatable.len(),
    );

    let sql = "SELECT status, COUNT(*) AS n FROM tsbench GROUP BY status ORDER BY n DESC LIMIT 10";
    let sql_runs = (reads / 100).clamp(1, 20);
    let mut samples = Vec::with_capacity(sql_runs);
    let mut sql_rows = 0usize;
    for _ in 0..sql_runs {
        let op = Instant::now();
        sql_rows = nonodb_core::sql::execute(&ts, sql, &[])?.len();
        samples.push(op.elapsed());
    }
    report_latency(
        &format!("SQL GROUP BY status ({sql_rows} groups)"),
        samples,
        sql_runs,
    );

    // Deletion bitmap and its effect on the on-disk size.
    let before_delete = ts.timeseries_stats()?.unwrap_or_default();
    let compacted_after_delete = dir.path().join("after-delete.mdb");
    let file = store.copy_to_file(
        &compacted_after_delete,
        nonodb_core::heed::CompactionOption::Enabled,
    )?;
    file.sync_all()?;
    println!(
        "deleted_rows={} (bitmap), compacted copy {:.2} MB",
        before_delete.deleted_rows,
        std::fs::metadata(&compacted_after_delete)?.len() as f64 / 1_048_576.0
    );

    // ------------------------------------------------------------------
    // Standard collection baseline
    // ------------------------------------------------------------------
    println!("\nbaseline (standard collection, one document per row)");
    let standard = store.create_collection("stdbench", nonodb_core::CollectionProfile::Standard)?;
    let started = Instant::now();
    let mut std_insert = Vec::with_capacity(rows);
    for idx in 0..rows {
        let op = Instant::now();
        standard.insert_one(doc(idx % series, idx))?;
        std_insert.push(op.elapsed());
    }
    let std_total = started.elapsed();
    report_latency("insert (standard)", std_insert, rows);
    println!(
        "{:<34} {:>7}    {:>9.0} rows/s (wall)",
        "insert wall clock",
        rows,
        rows as f64 / std_total.as_secs_f64()
    );

    let mut samples = Vec::with_capacity(scans);
    for _ in 0..scans {
        let op = Instant::now();
        let _ = standard.find(&query(json!({}), None))?;
        samples.push(op.elapsed());
    }
    report_latency("find match-all (standard)", samples, scans);

    let mut samples = Vec::with_capacity(scans);
    for _ in 0..scans {
        let op = Instant::now();
        let _ = standard.find(&query(wide.clone(), None))?;
        samples.push(op.elapsed());
    }
    report_latency("find 1 h, 1 series (standard)", samples, scans);

    let standard_sample = standard
        .find(&query(json!({}), Some(1)))?
        .into_iter()
        .next()
        .ok_or("no standard sample")?;
    let standard_id = standard_sample["_id"]
        .as_str()
        .ok_or("no standard id")?
        .to_string();
    let mut samples = Vec::with_capacity(reads);
    for _ in 0..reads {
        let op = Instant::now();
        assert!(standard.find_by_key(&standard_id)?.is_some());
        samples.push(op.elapsed());
    }
    report_latency("find_by_key (standard)", samples, reads);

    let mut samples = Vec::with_capacity(reads / 4);
    for _ in 0..(reads / 4).max(1) {
        let op = Instant::now();
        standard.count(&wide)?;
        samples.push(op.elapsed());
    }
    report_latency(
        "count 1 h, 1 series (standard)",
        samples,
        (reads / 4).max(1),
    );

    let mut samples = Vec::with_capacity(agg_runs);
    for _ in 0..agg_runs {
        let op = Instant::now();
        let _ = standard.aggregate(&pipeline)?;
        samples.push(op.elapsed());
    }
    report_latency(
        "aggregate $group 1 h, 1 series (standard)",
        samples,
        agg_runs,
    );

    let std_update_ids: Vec<String> = standard
        .find(&query(json!({"status": "ok"}), Some(update_sample)))?
        .iter()
        .filter_map(|doc| doc["_id"].as_str().map(str::to_string))
        .collect();
    let mut samples = Vec::with_capacity(std_update_ids.len());
    for id in &std_update_ids {
        let op = Instant::now();
        standard.update(&json!({"_id": id}), &json!({"$set": {"level": 9}}), false)?;
        samples.push(op.elapsed());
    }
    report_latency(
        &format!(
            "update $set by _id, {} rows (standard)",
            std_update_ids.len()
        ),
        samples,
        std_update_ids.len(),
    );

    let std_delete_ids: Vec<String> = standard
        .find(&query(json!({}), Some(delete_sample)))?
        .iter()
        .filter_map(|doc| doc["_id"].as_str().map(str::to_string))
        .collect();
    let mut samples = Vec::with_capacity(std_delete_ids.len());
    for id in &std_delete_ids {
        let op = Instant::now();
        standard.delete(&json!({"_id": id}), false)?;
        samples.push(op.elapsed());
    }
    report_latency(
        &format!("delete by _id, {} rows (standard)", std_delete_ids.len()),
        samples,
        std_delete_ids.len(),
    );

    let std_index_started = Instant::now();
    standard.create_index("status", false)?;
    println!(
        "{:<34} {:>9.2?}",
        "createIndex status (standard)",
        std_index_started.elapsed()
    );
    let mut samples = Vec::with_capacity((reads / 10).max(1));
    for _ in 0..(reads / 10).max(1) {
        let op = Instant::now();
        let _ = standard.find(&indexed)?;
        samples.push(op.elapsed());
    }
    report_latency(
        "find indexed status+series (standard)",
        samples,
        (reads / 10).max(1),
    );

    // Retention: drop the oldest hourly window of every series.
    let cutoff = base + 3_600_000;
    let retain_started = Instant::now();
    let report = store.retain_timeseries("tsbench", cutoff)?;
    let retain_total = retain_started.elapsed();
    println!(
        "\nretention before +1 h: {:?}, dropped_buckets={}, tombstoned={}, dropped_rows={}, remaining_rows={}",
        retain_total,
        report.dropped_buckets,
        report.tombstoned_buckets,
        report.dropped_rows,
        report.remaining_rows
    );

    println!(
        "\ndata directory size: {:.1} MB",
        directory_size(dir.path())? as f64 / 1_048_576.0
    );
    Ok(())
}

fn directory_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total += directory_size(&entry.path())?;
        } else {
            total += meta.len();
        }
    }
    Ok(total)
}
