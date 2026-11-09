# Benchmarks

This document collects the performance and resource measurements taken during
nonodb development. It is a reference point, not a guarantee. Storage latency,
CPU frequency, kernel version, filesystem, page cache, and client behavior all
affect the numbers.

## Test environment

| Component | Value |
|---|---|
| Date | 2026-11-09 (UTC) |
| OS / kernel | Linux 6.14.0 x86_64 |
| CPU | Intel Core Processor (Haswell, no TSX) |
| CPU count | 8 vCPU |
| RAM | 22.9 GiB |
| Data filesystem | `/dev/sda1`, ext4 |
| `/tmp` | tmpfs (used only for comparison) |
| Rust | rustc/cargo 1.99.1 |
| Python | 3.13.3 |
| `orjson` | 3.13.0 |
| uvicorn | 0.52.4 |
| Build | `cargo build --release` |
| Daemon binary | ~4.1 MB |

Unless stated otherwise:

- the database is on ext4, not tmpfs;
- clients are separate Python processes using one Unix-socket connection each;
- the daemon and clients run on the same host;
- measurements include HTTP/JSON and client-side overhead;
- writes use group commit with real `fsync`.

## Methodology and caveats

- **Use the target filesystem.** tmpfs makes `fsync` almost free and overstates
  write throughput. All primary results use ext4.
- **Release build.** Debug builds are not representative.
- **Separate processes.** The benchmark harness uses process pools to model a
  preforking web server, not threads in one interpreter.
- **Latency includes transport.** Index lookups are reported as end-to-end
  request latency, so the storage engine itself is faster than the p50.
- **Run-to-run variance.** Write throughput varies by roughly ±10% because of
  `fsync` and scheduling.
- **Page cache matters.** First runs after restart can be slower. Indexed
  queries remain fast once the relevant pages are cached.
- **Different tables come from different runs.** Each table states its data
  size; do not combine numbers from different rows as if they were one run.
- **Full scans are linear.** Indexed queries are effectively flat with data
  size; scans are not.

## Code quality and test status

At the time of the latest run:

| Check | Result |
|---|---|
| `cargo build --release --examples` | OK |
| `cargo fmt --check` | OK |
| `cargo clippy --all-targets -- -D warnings` | 0 warnings |
| `cargo test` | 49 unit + 5 property-based + 1 reopen test, all passing |
| End-to-end scripts | CRUD, cache, batch, write batch, upsert, search, compound indexes, uvicorn, crash, Docker |

The test suite covers filters, updates/upserts, paths, index encoding, unique
constraints, compact/covering indexes, query planning, BM25, fuzzy matching,
Top-K stability, range bounds, JSON parser compatibility, and property-based
checks for encoding and basic query semantics.

## CRUD throughput: 12 clients on ext4

`scripts/bench.py`, 12 client processes, 24,000 writes, 96,000 reads, release
build, 1M-document database for the large-data variant:

| Operation | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| `insert` (group commit) | **3,334 ops/s** | 3.21 ms | 6.02 ms | 10.73 ms |
| `find_one({_id})` | **6,833 ops/s** | 1.30 ms | 3.24 ms | 6.02 ms |

After the run:

- `avg_batch_size` ≈ 7.60
- `avg_commit_us` ≈ 728.6 µs
- `write_batches_total` = 3,156 instead of 24,000 individual commits
- `errors_total` = 0

Writes took 24,000 / 3,334 ≈ **7.20 s**. Reads took 96,000 / 6,833 ≈
**14.05 s**.

The read path is dominated by HTTP/JSON and Python client overhead, not LMDB.
The write path is dominated by `fsync` and the single-writer model.

### tmpfs comparison

Same workload on tmpfs, where `fsync` is much cheaper:

| Operation | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| `insert` | **4,602 ops/s** | 2.25 ms | 4.54 ms | 7.67 ms |
| `find_one({_id})` | **7,280 ops/s** | 1.26 ms | 3.18 ms | 5.92 ms |

`avg_batch_size` ≈ 8.65, `avg_commit_us` ≈ 54 µs. The ~28% write difference is
mostly the cost of real durable commits.

## Group commit scaling

12 vs 80 concurrent clients, ext4:

| Clients | Writes/s | `avg_batch_size` | `queue_peak` | `write_batches_total` |
|---|---|---|---|---|
| 12 | 3,404 | 7.19 | 12 | 3,368 |
| 80 | **3,856** | **24.87** | 80 | **965** |

With 80 clients, the adaptive 3 ms window increased the number of requests per
commit and reduced commits from roughly 3,000 to 965, improving throughput at
the cost of queueing latency.

## Concurrency correctness: 12 independent sources

`scripts/concurrency_test.py`: 12 processes, each with its own connection,
ext4.

| Scenario | Result |
|---|---|
| 12 writers × 300 inserts | 3,600 documents; 300 per source; unique `_id`s |
| 12 readers × 500 `_id` reads | 6,000/6,000 OK, 0 errors |
| Reads by unindexed field | 0 errors |
| Reads by indexed field | 0 errors; results identical to unindexed scan |
| Range `$gte/$lt` + sort + skip/limit | Correct, ordered |
| 12 sources × 100 `$inc` on one document | Final `n = 1200` |
| Unique index, 12 × 50 same keys | 50 successes, 550 `duplicate_key` |
| Mixed 900 writes + 1,800 reads | 0 errors |
| Final collection count | 4,551 as expected |
| Total scenario time | 7.3 s |

The 550 errors in `/metrics` are expected unique-index rejections, not failures.

## Read batches

`scripts/opt_bench.py`, 1,000 `find_one` calls by `_id`, 50,000 documents, ext4:

| Method | Time | Throughput |
|---|---|---|
| Individually (1,000 HTTP requests) | 639.7 ms | 1,563 ops/s |
| Batch of 100 (10 requests) | **19.1 ms** | **52,252 ops/s** |

Approximately **33× faster** with `orjson` (about 24× without it). A read batch
also runs in one LMDB snapshot, which is often as valuable as the throughput
gain.

On 1M / 2M / 4M / 8M documents, 1,000 batched `find_one` calls took
**22.7 / 23.9 / 20.0 / 19.3 ms** respectively. Batch performance is dominated by
request/response serialization rather than database size.

## Search performance

### 5,000 documents

`scripts/search_test.py`; ~20 tokens per document, ext4, single sequential
client, end-to-end through the daemon:

| Operation | Time |
|---|---|
| `insert_many` 5,000 documents (no search indexes) | 0.107 s |
| `create_text_index` backfill | 0.101 s |
| `create_fuzzy_index` backfill | 0.115 s |
| BM25 `search`, 500 queries | p50 **4.23 ms**, p95 **7.54 ms** |
| `fuzzy`, 200 queries | p50 **4.83 ms**, p95 **8.07 ms** |
| `data.mdb` with both indexes | 18 MB |

### 100,000 documents (in a 1M database)

| Operation | Result |
|---|---|
| Text index build, 100k docs | 0.8 s |
| Fuzzy index build, 100k docs | 1.9 s |
| BM25 search, limit 10 | p50 **62 ms** |
| Fuzzy search, max distance 2 | p50 **40 ms** |

### 1,000,000 text documents (in an 8M database)

| Operation | Result |
|---|---|
| Load 1M text documents | 14.9 s |
| `create_text_index` backfill | 12.5 s |
| `create_fuzzy_index` backfill | 28.2 s |
| BM25 search, limit 10 | p50 **971 ms**, p95 1,025 ms |
| Fuzzy search, max distance 2 | p50 **168 ms**, p95 237 ms |

BM25 at 1M text documents is a full posting-list scan and ranking operation;
the text index makes candidate lookup possible but does not make large OR
queries free. Fuzzy search is cheaper here because trigram candidate selection
limits the number of documents scored.

## Large data: 1,000,000 documents

`scripts/large_test.py --docs 1000000 --text`, fresh database, ext4, release.
Each document is roughly 150 bytes: `_id`, `grp`, `age`, `score`, `city`,
`name`, `payload`. Indexes: `grp`, `age`, `score`; a separate text collection
has 100,000 documents.

| Phase | Time | `data.mdb` |
|---|---|---|
| 1,000,000 inserts (`insert_many` chunks of 500) | **13.3–13.6 s** (73.7k–75.1k docs/s) | 242 MB |
| Backfill 3 indexes (parallel) | **3.4–3.7 s** | 445–465 MB |
| +100,000 text docs and 2 search indexes | ~5 s | ~627 MB |

Query latency p50/p95:

| Query | p50 | p95 |
|---|---|---|
| `find_one` by `_id` | **0.56 ms** | 1.18 ms |
| `find(grp=x, limit=10)` via index | **0.77 ms** | 1.41 ms |
| `find(age in [18,30), limit=10)` via range | **0.58 ms** | 0.91 ms |
| `count({})` | **0.50 ms** | 0.87 ms |
| `count(grp=x)` via index | **0.62 ms** | 0.86 ms |
| `find(grp=x, skip=500, limit=10)` | 2.07 ms | 2.72 ms |
| `find(grp=x, sort age, limit=10)` | 4.29 ms | 5.59 ms |
| `find_one(name=...)` **without index** | **512 ms** | 983 ms |
| 1,000 batched `find_one` calls | **22.7 ms** | — |

Bulk operations:

- `update(grp=7, multi=true)` over 1,000 documents: **50 ms**
- `delete(grp=8)` over 1,000 documents: **50 ms**
- Insert of a 900 KiB document: **25.9 ms**
- Read of that document: **5.9 ms**

## Optimization impact at 1M documents

| Query | Before | After | Improvement |
|---|---|---|---|
| `count(age ∈ [18,30))` | 1.07 s | **5.6 ms** | ~190× |
| `find({}, skip=500000, limit=10)` | 1.40 s | **36.6 ms** | ~38× |
| `find({}, sort score, limit=10)` | 7.18 s | **1.29 s** (then 250–420 ms with parallel sort) | ~5.6× initially |
| `find(age range, sort score, limit=10)` | 2.58 s | **321 ms** | ~8× |
| `find(age range, limit=10)` | 0.58 ms | 0.58 ms | no regression |

The main changes:

- bounded top-K heaps instead of full result collection and sort;
- `skip` without parsing skipped documents;
- combined index range bounds with type awareness;
- limiting index candidates to `skip + limit`;
- index-only count without materializing ids;
- parallel full scans for shallow sorted queries;
- `sonic-rs` parsing on read paths.

## 2M, 4M, and 8M documents

Documents ~150 bytes, indexes `grp`, `age`, `score`. `NONODB_MAP_SIZE` was 4 GiB
for 2M/4M and 8 GiB for 8M. ext4, release.

| Metric | 2M | 4M | 8M |
|---|---|---|---|
| Load (`insert_many` chunks of 1,000) | 28.8 s (69,419/s) | 51.0 s (78,366/s) | **89.0 s (89,911/s)** |
| `data.mdb` after 3 indexes | 915 MB | 1.83 GB | 3.50 GB |
| Backfill 3 indexes | 13.1 s | 28.0 s | ~17–21 s per index |
| RSS after indexes | 750 MB | 1.49 GB | 3.32 GB (page cache) |

Indexed query latency p50:

| Query | 2M | 4M | 8M |
|---|---|---|---|
| `find_one` by `_id` | 0.59 ms | 0.53 ms | **0.51 ms** |
| `find(grp=x, limit=10)` | 0.68 ms | 0.65 ms | **0.62 ms** |
| `find(age range, limit=10)` | 0.61 ms | 0.61 ms | **0.56 ms** |
| `count({})` | 0.54 ms | 0.60 ms | **0.60 ms** |
| `count(grp=x)` | 0.68 ms | 0.90 ms | **1.48 ms** |
| `find(grp=x, skip=500, limit=10)` | 2.60 ms | 2.60 ms | **2.75 ms** |
| `find(grp=x, sort age, limit=10)` | 6.72 ms | 15.28 ms | **24.74 ms** |
| 1,000 batched `find_one` | 23.9 ms | 20.0 ms | **19.3 ms** (51.8k ops/s) |

Indexed point lookups are effectively flat from 1M to 8M. Compound index sort
latency grows with the number of matching entries but remains in the tens of
milliseconds.

Full scans are linear:

| Operation | 2M | 4M | 8M |
|---|---|---|---|
| `count(age ∈ [18,30))` via index | 9.2 ms | 20.2 ms | **51.2 ms** |
| `find({}, skip=N/2, limit=10)` | 64.1 ms | 141.8 ms | **298.7 ms** |
| `find({}, sort score, limit=10)` (older serial run) | 2.34 s | 4.93 s | **8.63 s** |
| `find(age range, sort score, limit=10)` (older serial run) | 0.71 s | 1.42 s | **2.60 s** |

The sorted full-scan numbers for 2M/4M/8M predate the parallel sort path. The
parallel path is measured separately below.

Concurrent 12-process workload (24,000 writes + 96,000 reads):

| Database | Write throughput | Read throughput |
|---|---|---|
| 4M | 3,534 ops/s (p50 2.95 ms) | 6,983 ops/s (p50 1.31 ms) |
| 8M | **3,187 ops/s** (p50 3.24 ms) | **7,669 ops/s** (p50 1.24 ms) |

## CPU usage and parallel sorting

`scripts/cpu_bench.py` samples `/proc/<daemon>/stat` every 50 ms during separate
write and read phases, 12 client processes, 1M database.

| Phase | Throughput | Daemon CPU |
|---|---|---|
| Single-document writes | 3,570 ops/s | **0.37 cores** (writer 0.14 + tokio 0.21) |
| `_id` reads, 12 clients | 7,110–7,504 ops/s | **0.57–0.66 cores** |
| `_id` reads, 4 clients | 4,390 ops/s | 0.50 cores |

Normal CRUD is not CPU-bound. The bottleneck is transport, Python, and
`fsync`. Parallelism matters for CPU-bound full scans and sorting.

Parallel full scan (`sort score, limit=10`, no usable index):

| Database | Serial before | Parallel after | Daemon CPU |
|---|---|---|---|
| 1M | ~1,050 ms | **237–327 ms** | 4.4–5.0 cores |
| 4M | 4,935 ms | **729–782 ms** | 6.2–6.4 cores |

Speedup: **3.2–4.4×** at 1M and **6.3–6.8×** at 4M. Results are identical to
the serial path, including stable ordering on ties.

After the `sonic-rs` read-path change, full sort at 1M was measured at
**250–420 ms** for different fields, including cold-cache runs.

## Latest v1.1 feature measurements

Same host, ext4, 1M documents, latest feature set:

| Operation | Result |
|---|---|
| Load 1M | 13.3 s (75,143 docs/s); 242 MB |
| Backfill 3 indexes (parallel) | 3.4 s; file 445 MB |
| Text index, 100k | 0.8 s |
| Fuzzy index, 100k | 1.9 s |
| Full sort 1M (`age`, limit 10) | 320.7 ms |
| Full sort 1M (`name`, limit 10) | 251.7 ms |
| Index-only sort (`grp >= 0`, limit 10) | **1.0 ms** |
| Index-only sort + `skip=500` | **2.3 ms** |
| Range filter + index sort | **3.5 ms** |
| Batch 1,000 `find_one` | 20.0 ms |
| BM25 100k | p50 62 ms |
| Fuzzy 100k | p50 40 ms |

## Memory

Representative daemon memory on the benchmark host:

| State | RSS | PSS |
|---|---|---|
| 1,000 documents, 3 indexes, idle | ~5.1–5.4 MB | ~2.6–3.0 MB |
| 1M documents, 3 indexes, idle after restart | ~5.1 MB | ~2.9 MB |
| 1M documents, after a few reads | ~6.4 MB | ~4.0 MB |
| 1M documents, after a full scan + sort | ~278 MB | ~276 MB |
| 1M documents, peak during parallel backfill | ~552 MB | — |
| 100k text/fuzzy documents | up to ~788 MB peak | — |
| 10,000 small documents (earlier test) | ~10 MB | — |

Virtual size (`VmSize`) is typically 1.6–2.2 GB because `map_size` is 1 GiB and
libraries are mapped. This is address space, not resident memory. At 8M
documents, RSS after index builds was 3.32 GB, mostly page cache for mapped
pages.

## Crash safety

Test: 1M+ database, 12 processes × 20,000 inserts in chunks of 200, `kill -9`
after 1.2 s.

| Check | Result |
|---|---|
| Documents before crash | 1,030,200 |
| Documents after restart | **1,064,200** |
| Newly committed before kill | 34,000 (multiple of 200) |
| Restart + health check | **17 ms** |
| Index rebuild counts after restart | Identical |

The committed count was a multiple of the insert chunk size, confirming
per-batch commits. No corruption or inconsistency was observed.

## Backup and restore

Live backup with compaction:

| Database | Source → backup | Time | Throughput |
|---|---|---|---|
| 1.023M documents | 685 MB → 615 MB | **1.15 s** | ~600 MB/s |
| 8M documents + 1M text | 5.1 GB → ~5.0 GB | **9.95 s** | ~510 MB/s |

After restoring the 1M backup, counts, indexes, BM25, and fuzzy search all
verified correctly. After restoring the 8M backup, counts, 3 indexes, text
search, and BM25 verified correctly.

## Interpreting the results

- **Indexed point/range reads scale.** 0.5–1.5 ms from 1M to 8M for the tested
  workload.
- **Batches are the biggest client-side win.** One thousand `find_one` calls
  drop from hundreds of milliseconds to about 20 ms.
- **Writes are bounded by durability.** One writer plus `fsync` gives ~3.2–3.9k
  writes/s with real disk commits on this host.
- **Full scans are linear.** Sorting 8M documents without an index is seconds,
  not milliseconds. Use an index or a covered query.
- **Search has its own scaling curve.** BM25 over 1M text documents is hundreds
  of milliseconds; the inverted index avoids scanning every document's text but
  does not make large term lists free.
- **RSS is usually small.** Large RSS values after scans are mostly mapped page
  cache, not leaked allocations.
- **Map size is not memory.** A 1 GiB `map_size` does not mean 1 GiB of RAM.

## Reproducing

The repository includes the benchmark and test scripts under `scripts/`.
Representative commands:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --release --examples
cargo test
cargo clippy --all-targets -- -D warnings

# Put benchmark data on a real disk, not tmpfs.
D=$PWD/.bench
S=$D/nonodb.sock
rm -rf "$D"
mkdir -p "$D/data"
NONODB_DATA=$D/data NONODB_SOCK=$S ./target/release/nonodb-daemon &

NONODB_SOCK=$S python3 scripts/bench.py --workers 12 --writes 24000 --reads 96000
NONODB_SOCK=$S python3 scripts/opt_bench.py --docs 50000
NONODB_SOCK=$S python3 scripts/batch_test.py
NONODB_SOCK=$S python3 scripts/search_test.py
NONODB_SOCK=$S python3 scripts/search_variants_test.py
NONODB_SOCK=$S python3 scripts/compound_test.py
NONODB_SOCK=$S python3 scripts/upsert_test.py
NONODB_SOCK=$S python3 scripts/write_batch_test.py
```

Large-data tests need a fresh database and larger map size:

```bash
NONODB_DATA=$D/data NONODB_SOCK=$S NONODB_MAP_SIZE=8GiB \
  ./target/release/nonodb-daemon &

NONODB_DATA=$D/data NONODB_SOCK=$S NONODB_PID=$! \
  python3 scripts/large_test.py --docs 1000000 --text

NONODB_DATA=$D/data NONODB_SOCK=$S NONODB_PID=$! \
  bash scripts/large_crash_test.sh
```

Crash and uvicorn tests:

```bash
BIN=./target/release/nonodb-daemon bash scripts/crash_test.sh
BIN=./target/release/nonodb-daemon bash scripts/uvicorn_test.sh
```

Benchmark results are sensitive to the host, filesystem, kernel, and page
cache. Treat these numbers as a baseline to compare against your own hardware.
