//! Disk usage report for time-series collections.
//!
//! Measures the LMDB `data.mdb` size for the same 100k-row workload stored as:
//!
//! 1. time-series buckets, unsealed (pending rows are raw JSON);
//! 2. time-series buckets, sealed (columnar);
//! 3. after a retention pass (LMDB does not shrink the file);
//! 4. a compacted copy (`copy_to_file` with compaction), which is what
//!    `nonodb-cli backup` produces;
//! 5. a standard document-per-row collection as the baseline.
//!
//! Run with:
//!
//! ```text
//! cargo run --release -p nonodb-core --example timeseries_space
//! ```

use std::path::Path;
use std::time::Instant;

use serde_json::{json, Value};

use nonodb_core::heed::CompactionOption;
use nonodb_core::Store;

fn data_mdb(dir: &Path) -> u64 {
    std::fs::metadata(dir.join("data.mdb"))
        .map(|meta| meta.len())
        .unwrap_or(0)
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / 1_048_576.0
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows: usize = std::env::var("ROWS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(100_000);
    let series = 10usize;
    println!("nonodb time-series disk usage: rows={rows} series={series}\n");

    // ------------------------------------------------------------------
    // Time-series store
    // ------------------------------------------------------------------
    let ts_dir = tempfile::tempdir()?;
    let ts_store = Store::open(ts_dir.path())?;
    let settings = nonodb_core::timeseries::Settings {
        time_field: "ts".into(),
        meta_field: Some("device".into()),
        ..Default::default()
    };
    let ts = ts_store.create_timeseries_collection("bench", &settings)?;
    let started = Instant::now();
    for idx in 0..rows {
        ts.insert_one(doc(idx % series, idx))?;
    }
    let insert_time = started.elapsed();
    let raw_per_row = {
        let stats = ts.timeseries_stats()?.unwrap_or_default();
        stats.raw_bytes as f64 / rows as f64
    };
    let unsealed = data_mdb(ts_dir.path());

    let seal_started = Instant::now();
    ts_store.seal_timeseries("bench")?;
    let seal_time = seal_started.elapsed();
    let sealed = data_mdb(ts_dir.path());
    let stats = ts.timeseries_stats()?.unwrap_or_default();

    println!("time-series collection");
    println!(
        "  insert:            {:?} ({:.0} rows/s)",
        insert_time,
        rows as f64 / insert_time.as_secs_f64()
    );
    println!("  seal:              {:?}", seal_time);
    println!(
        "  raw JSON:          {:.2} MB ({:.2} B/row)",
        stats.raw_bytes as f64 / 1_048_576.0,
        raw_per_row
    );
    println!(
        "  data.mdb unsealed: {:.2} MB ({:.2} B/row)",
        mb(unsealed),
        unsealed as f64 / rows as f64
    );
    println!(
        "  data.mdb sealed:   {:.2} MB ({:.2} B/row)",
        mb(sealed),
        sealed as f64 / rows as f64
    );
    println!(
        "  compression:       {:.1}x vs raw JSON",
        stats.raw_bytes as f64 / sealed.max(1) as f64
    );

    let compacted_full_path = ts_dir.path().join("compacted-full.mdb");
    let file = ts_store.copy_to_file(&compacted_full_path, CompactionOption::Enabled)?;
    file.sync_all()?;
    let compacted_full = std::fs::metadata(&compacted_full_path)?.len();
    println!(
        "  data.mdb compacted:      {:.2} MB ({:.2} B/row for {} rows)",
        mb(compacted_full),
        compacted_full as f64 / rows as f64,
        rows
    );

    // Retention: drop the first half of the time range.
    let cutoff = 1_700_000_000_000i64 + 5_000_000;
    let retain_started = Instant::now();
    let report = ts_store.retain_timeseries("bench", cutoff)?;
    let retain_time = retain_started.elapsed();
    let after_retention = data_mdb(ts_dir.path());
    println!(
        "  retention:         {:?}, dropped {} buckets + {} rewritten, {} rows left",
        retain_time, report.dropped_buckets, report.rewritten_buckets, report.remaining_rows
    );
    println!(
        "  data.mdb after retention: {:.2} MB (LMDB keeps the high-water mark)",
        mb(after_retention)
    );

    let compacted_path = ts_dir.path().join("compacted.mdb");
    let file = ts_store.copy_to_file(&compacted_path, CompactionOption::Enabled)?;
    file.sync_all()?;
    let compacted = std::fs::metadata(&compacted_path)?.len();
    println!(
        "  data.mdb compacted:      {:.2} MB ({:.2} B/row of the {} remaining rows)",
        mb(compacted),
        compacted as f64 / report.remaining_rows.max(1) as f64,
        report.remaining_rows
    );

    // ------------------------------------------------------------------
    // Standard collection baseline (same documents, no indexes)
    // ------------------------------------------------------------------
    let std_dir = tempfile::tempdir()?;
    let std_store = Store::open(std_dir.path())?;
    let standard =
        std_store.create_collection("bench", nonodb_core::CollectionProfile::Standard)?;
    for idx in 0..rows {
        standard.insert_one(doc(idx % series, idx))?;
    }
    let std_size = data_mdb(std_dir.path());
    println!("\nstandard collection (one document per row, no indexes)");
    println!(
        "  data.mdb:          {:.2} MB ({:.2} B/row)",
        mb(std_size),
        std_size as f64 / rows as f64
    );
    println!(
        "  compacted TS is {:.1}x smaller on disk ({:.2} MB vs {:.2} MB)",
        std_size as f64 / compacted_full.max(1) as f64,
        mb(compacted_full),
        mb(std_size)
    );
    println!(
        "  raw JSON is {:.1}x larger than the compacted bucket file",
        stats.raw_bytes as f64 / compacted_full.max(1) as f64
    );
    Ok(())
}
