//! Build and query one immutable `search_segment` file from a TSV corpus.
//!
//! ```bash
//! cargo run --release -p nonodb-core --example segment_bench -- \
//!   collection.tsv /var/lib/vz/.../msmarco.seg \
//!   --queries queries.train.tsv --samples 500 --limit 10
//! ```
//!
//! Pass `--rebuild` to replace an existing segment, or omit the build inputs to
//! query an existing segment.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use nonodb_core::search_segment::{SegmentBuilder, SegmentReader};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let corpus = args.next().ok_or("missing corpus TSV")?;
    let segment = PathBuf::from(args.next().ok_or("missing segment path")?);
    let mut queries: Option<PathBuf> = None;
    let mut samples = 500usize;
    let mut limit = 10usize;
    let mut rebuild = false;
    let mut max_docs = 0usize;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--queries" => {
                queries = Some(PathBuf::from(args.next().ok_or("missing --queries value")?))
            }
            "--samples" => samples = args.next().ok_or("missing --samples value")?.parse()?,
            "--limit" => limit = args.next().ok_or("missing --limit value")?.parse()?,
            "--max-docs" => max_docs = args.next().ok_or("missing --max-docs value")?.parse()?,
            "--rebuild" => rebuild = true,
            other => return Err(format!("unknown argument `{other}`").into()),
        }
    }

    if rebuild || !segment.exists() {
        build(&corpus, &segment, max_docs)?;
    }

    let reader = SegmentReader::open(&segment)?;
    println!(
        "segment={} documents={} terms={} bytes={}",
        segment.display(),
        reader.documents(),
        reader.terms(),
        std::fs::metadata(&segment)?.len()
    );

    let Some(query_path) = queries else {
        return Ok(());
    };
    let mut queries = load_queries(&query_path)?;
    if queries.is_empty() {
        return Err("no queries found".into());
    }
    if samples < queries.len() {
        let step = queries.len() / samples;
        queries = queries
            .into_iter()
            .step_by(step.max(1))
            .take(samples)
            .collect();
    }

    for query in queries.iter().take(10) {
        reader.search(query, limit)?;
    }
    let mut latencies = Vec::with_capacity(queries.len());
    let mut slowest = Vec::with_capacity(queries.len());
    let started = Instant::now();
    for query in &queries {
        let begin = Instant::now();
        let hits = reader.search(query, limit)?;
        let elapsed = begin.elapsed();
        latencies.push(elapsed);
        slowest.push((elapsed, query.clone(), hits.len()));
    }
    let wall = started.elapsed();
    latencies.sort_unstable();
    slowest.sort_by_key(|a| std::cmp::Reverse(a.0));
    println!(
        "queries={} p50={:.2}ms p95={:.2}ms p99={:.2}ms mean={:.2}ms qps={:.1}",
        queries.len(),
        pct(&latencies, 0.50),
        pct(&latencies, 0.95),
        pct(&latencies, 0.99),
        latencies.iter().map(|d| d.as_secs_f64()).sum::<f64>() * 1000.0 / latencies.len() as f64,
        queries.len() as f64 / wall.as_secs_f64()
    );
    for (elapsed, query, hits) in slowest.iter().take(5) {
        println!(
            "  {:9.2}ms hits={hits:2} {query}",
            elapsed.as_secs_f64() * 1000.0
        );
    }
    Ok(())
}

fn build(corpus: &str, segment: &Path, max_docs: usize) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(corpus)?;
    let mut reader = BufReader::with_capacity(1 << 20, file);
    let mut builder = SegmentBuilder::new(segment)?;
    let started = Instant::now();
    let mut docs = 0usize;
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        if max_docs != 0 && docs >= max_docs {
            break;
        }
        let Some((id, text)) = line.trim_end_matches('\n').split_once('\t') else {
            continue;
        };
        builder.add_document(id, text)?;
        docs += 1;
        if docs.is_multiple_of(1_000_000) {
            println!(
                "build {docs} docs at {:.1}s",
                started.elapsed().as_secs_f64()
            );
        }
    }
    let stats = builder.finish()?;
    println!(
        "built documents={} terms={} postings={} bytes={} elapsed={:.1}s",
        stats.documents,
        stats.terms,
        stats.postings,
        stats.bytes,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn load_queries(path: &Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut queries = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let (_qid, query) = line.split_once('\t').unwrap_or(("", &line));
        if !query.trim().is_empty() {
            queries.push(query.trim().to_string());
        }
    }
    Ok(queries)
}

fn pct(values: &[Duration], q: f64) -> f64 {
    let idx = ((values.len() - 1) as f64 * q).round() as usize;
    values[idx].as_secs_f64() * 1000.0
}
