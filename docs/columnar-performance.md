# Columnar collections: performance evidence

Status: release-gate evidence for the
[columnar acceptance criteria](columnar-collections-acceptance.md#12-performance-and-capacity-gates).

This report records the canonical standard-vs-columnar benchmark required by
`AC-PERF-001` through `AC-PERF-010`. It is intentionally a small summary rather
than a committed benchmark log; the reproducible harness prints raw results to
stdout and does not write them into the repository.

## Reproduction

```bash
cargo build --release -p nonodb-core --example columnar_bench
NONODB_AGGREGATE_MAX_DOCS=200000 ./target/release/examples/columnar_bench
```

The harness creates both profiles in temporary LMDB directories, inserts the
same documents, and measures the same operations in the same order. It is a raw
standard-vs-standard workload, not a columnar-optimized query against a
standard full scan.

Dataset: 100,128 append-heavy analytics documents with realistic string `_id`
values (`id-00000000`), nested objects, arrays, timestamps, floats, enums, and
booleans. Ingest is 100 transactions of 1,000 rows; buckets hold 512 rows. The
last 128 rows are inserted singly so the open-bucket gate measures a genuinely
open staging bucket rather than a sealed one. Two secondary indexes (`user`,
`status`) are created before the storage and retention measurements. History is
disabled for both profiles by default, TTL is unused, and both profiles run on
the same filesystem and LMDB map size.

Environment: Linux 7.0.14, Intel Xeon D-2141I, 14 GiB RAM, Rust 1.100.0,
`--release`, LMDB map size 1 GiB.

## Canonical result

One representative run:

| Metric | standard | columnar | gate | result |
|---|---:|---:|---:|---|
| Storage including ids, locator map, and 2 indexes | 36.1 MB | 17.6 MB | `columnar / standard <= 0.50` | **0.487** |
| Batch insert (1,000 rows / transaction) | 643.3 ms | 727.2 ms | `standard / columnar >= 0.80` | **0.885** |
| Single insert | 19.95 us | 38.29 us | `columnar / standard <= 2.0` (gate 0.50×) | **1.92x** |
| Sealed point read p50 | 4.9 us | 9.5 us | `columnar / standard <= 3.0` | **1.94x** |
| Open-bucket point read p50 | 4.5 us | 6.0 us | `columnar / standard <= 1.5` | **1.33x** |
| Full scan | 228.0 ms | 159.6 ms | `standard / columnar >= 0.80` | **1.43x** |
| `$group` + `$sort` aggregation | 91.0 ms | 107.0 ms | `standard / columnar >= 0.80` | **0.85x** |
| Single-document update p50 | 24.9 us | 33.5 us | `columnar / standard <= 5.0` | **1.35x** |
| Multi-document update, 37,527 rows | 718.3 ms | 1156.0 ms | reported separately | **0.62x throughput** |

Three runs after an untimed aggregation warmup produced these ranges: storage
`0.487-0.488`, batch `0.849-0.906`, single insert `1.90-1.98x`, sealed point
`1.94-2.15x`, open-bucket point `1.28-1.33x`, update `1.35-1.37x`, scan
`1.36-1.43x`, aggregation `0.805-0.920x`. The untimed warmup prevents
thread-startup and page-cache effects from dominating the aggregation ratio.
Multi-update throughput was `32.5k-33.4k rows/s` for columnar versus
`52.2k-54.2k rows/s` for standard in the same runs.

Retention/compaction (`AC-PERF-007`, `AC-PERF-010`): deleting 25% of rows left
18.2 MB; compaction reduced it to 15.6 MB (`reclaim = 0.860`). Bucket
seal/compaction p95 was 1.47-1.50 s, sealed point-read p95 was 11.9-12.6 us, and
open-bucket point-read p95 was 6.0-6.1 us. No correctness result changed after
compaction or reopen.

## Shared-codec regression check (`AC-PERF-008`)

The adaptive codec extraction was checked against the pre-extraction commit
`2e30bc2` (v0.34.0) with three runs of the same time-series benchmark:

```bash
git worktree add /tmp/nonodb-old 2e30bc2
(cd /tmp/nonodb-old && CARGO_TARGET_DIR=/tmp/old-target \
  cargo build --release -p nonodb-core --example timeseries_bench)
for run in 1 2 3; do
  ROWS=100000 SERIES=10 READS=2000 /tmp/old-target/release/examples/timeseries_bench
  ROWS=100000 SERIES=10 READS=2000 ./target/release/examples/timeseries_bench
done
```

The project noise band for these short runs is roughly ±5%. Observed ranges:

| Workload | v0.34.0 (3 runs) | branch (3 runs) | result |
|---|---:|---:|---|
| time-series insert | 16,252-16,521 ops/s | 16,004-16,599 ops/s | within noise |
| time-series bulk load (1k txns) | 35,189-35,842 rows/s | 35,101-37,249 rows/s | within noise |
| time-series point read | 2,734-2,921 ops/s | 2,717-2,908 ops/s | within noise |
| standard insert | 41,480-42,517 ops/s | 42,390-43,137 ops/s | within noise |
| standard bulk load | 84,279-86,130 rows/s | 81,986-87,913 rows/s | within noise |
| standard point read | 148,604-156,485 ops/s | 143,633-160,931 ops/s | within noise |

No standard or time-series metric moved outside the measured spread. The
shared codec keeps its byte-compatibility fixture (`timeseries` formatting is
unchanged), and the columnar profile adds no branch to the standard document
write path.

## Bounded-memory soak (`AC-PERF-006`)

`nonodb-core/examples/columnar_soak.rs` inserts a configurable number of rows in
1,000-row transactions into a columnar collection on real disk. It prints the
peak resident set size from `/proc/self/status` (`VmHWM`) every million rows.

```bash
NONODB_SOAK_DOCS=10000000 \
NONODB_SOAK_DIR=/path/on/disk \
NONODB_MAP_SIZE=4294967296 \
  cargo run --release -p nonodb-core --example columnar_soak
```

The 10,000,000-row insertion plus the final `storage_stats` call completed in
402.7 s and reported 2.05 GB of bucket data. Peak **anonymous** memory
(`RssAnon`) stayed at **9 MiB** for every million-row checkpoint; the write path
only holds one transaction's staging buffer, and `storage_stats` reads bucket
headers instead of materializing every document. Peak total RSS reached
603 MiB because LMDB maps the 4 GiB data file and touched pages are counted as
file-backed RSS; the kernel can reclaim those pages and they do not grow the
process heap. Run this harness on a disk path, not a small tmpfs, so the LMDB
map file does not compete with RAM.

## Interpretation

- Storage passes because sealed buckets use the shared adaptive codec, the
  columnar index entries store the id once in a compact key trailer instead of
  duplicating it in the value, and compaction re-encodes row-oriented staging
  buckets into typed columns.
- Batch insert passes because the write path buffers a whole transaction,
  serializes rows in parallel, appends a row-oriented staging bucket, and skips
  index/graph/TTL hooks that cannot apply to an empty index set.
- Scan and aggregation pass because raw staging buckets are decoded in
  parallel with document-count-balanced chunks; order-independent simple
  `$group` specs use a typed accumulator path instead of a generic document
  evaluator.
- Point reads and updates are intentionally slower than standard: they resolve
  an id hash to a locator and decode one column block, which remains within the
  accepted `3x`/`5x` gates.

## Pathological cases (`AC-PERF-009`)

The design falls back safely rather than silently choosing a worst case:

- schema drift: each bucket owns its field dictionary; missing fields use null
  bitmaps;
- high string cardinality: the adaptive codec keeps raw/dictionary/prefix
  candidates and picks the smallest; compaction re-evaluates the choice;
- large arrays and nested values: arrays are stored as JSON leaf columns;
- large ids: the compact index trailer switches from one byte to a tagged
  `u32` length, and bucket rows still carry the exact id;
- oversized rows: a row that cannot fit the 4 MiB hard bucket limit is rejected
  with the documented `DocumentTooLarge` path instead of truncating.
