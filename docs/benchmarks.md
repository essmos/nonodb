# Benchmarks

This document collects the performance and resource measurements taken during
nonodb development. It is a reference point, not a guarantee. Storage latency,
CPU frequency, kernel version, filesystem, page cache, and client behavior all
affect the numbers.

## Test environment

| Component | Value |
|---|---|
| Date | 2026-11-11 (UTC) |
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
| `cargo test` | 65 core unit + 7 daemon tests + 5 property-based + 1 reopen test, all passing |
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

### Document history overhead

`scripts/bench.py`, 8 clients, 8,000 writes, 20,000 reads, history retention
`NONODB_HISTORY_MAX_VERSIONS=5`, ext4:

| Configuration | Writes/s | `find_one` reads/s | Median write latency |
|---|---:|---:|---:|
| History disabled | **3,432** | 6,207 | 2.00 ms |
| History enabled | **3,051** | 6,165 | 2.13 ms |

History added about **11% write overhead** in this small run and no measurable
read overhead. More history retention, larger documents, or update-heavy
workloads increase write amplification and storage use.

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

## Ordered point lookups

`scripts/lookup_bench.py`, release daemon, `NONODB_CACHE_TTL=0`, ext4,
1,000,000 documents, 20% missing keys, single Unix-socket client. The collection
had a unique index on `email` with `include=["name","status"]`. Warmup was
excluded; each request fetched 1, 100, or 1,000 keys.

Setup:

| Workload | Result |
|---|---|
| Load 1M documents | 15.05 s, **66,440 docs/s** |
| Build `unique(email)` with `include(name,status)` | 4.06 s, **246,293 docs/s** |
| `data.mdb` | 440.1 MiB |

Latency is end-to-end through HTTP/JSON/UDS and includes the Python client.
Throughput is keys per second, so missing keys still count as processed input.

| Workload | Keys/request | p50 | p95 | p99 | Key throughput |
|---|---:|---:|---:|---:|---:|
| `lookup(_id)`, full documents | 1 | 0.67 ms | 1.20 ms | 2.10 ms | 1,202 keys/s |
| `lookup(_id)`, full documents | 100 | 1.73 ms | 3.29 ms | 5.79 ms | 45,457 keys/s |
| `lookup(_id)`, full documents | 1,000 | **10.46 ms** | 17.83 ms | 40.85 ms | **84,743 keys/s** |
| `lookup(email)`, covering `name,status` | 1 | 0.80 ms | 1.69 ms | 2.33 ms | 1,081 keys/s |
| `lookup(email)`, covering `name,status` | 100 | 1.34 ms | 2.65 ms | 5.98 ms | 60,337 keys/s |
| `lookup(email)`, covering `name,status` | 1,000 | **8.87 ms** | 14.77 ms | 41.28 ms | **98,527 keys/s** |
| `lookup(email)`, full documents | 1 | 0.72 ms | 2.07 ms | 3.90 ms | 896 keys/s |
| `lookup(email)`, full documents | 100 | 2.01 ms | 4.45 ms | 6.77 ms | 42,464 keys/s |
| `lookup(email)`, full documents | 1,000 | 13.47 ms | 20.38 ms | 25.77 ms | 70,759 keys/s |
| Old `batch(find_one(_id))` | 100 | 2.31 ms | 4.64 ms | 23.58 ms | 33,262 keys/s |
| Old `batch(find_one(_id))` | 1,000 | 19.01 ms | 31.46 ms | 49.24 ms | 47,878 keys/s |
| `lookup(_id)`, 1,000 keys, 50% duplicates | 1,000 | 9.62 ms | 18.62 ms | 24.78 ms | 89,540 keys/s |

Interpretation:

- At 1,000 keys, `lookup(_id)` is about **1.8x faster** than the previous
  `get_many` implementation built from batched `find_one` calls.
- The covering email lookup avoids loading documents: **8.87 ms** versus
  **13.47 ms** p50 for full documents, about **1.5x** higher key throughput.
- Per-key cost falls as the batch grows; request/HTTP overhead dominates at one
  key. The 1,000-key case processes roughly **85k–99k keys/s** on one client.
- Repeated keys are deduplicated internally. A 1,000-key request containing 500
  unique keys completed slightly faster than the all-unique case because it did
  half the index/document work while still returning 1,000 aligned results.
- Missing keys are returned as `null` and do not abort the request. A batch of
  1,000 keys with 20% misses completed in the same range as the all-present
  cases.
- The result budget is capped at 64 MiB; oversized results fail with
  `result_too_large`.
- These are single-client numbers. Concurrent throughput will depend on the
  read-only LMDB snapshots, CPU cores, and the JSON payload size. Run-to-run
  variance on this shared host was roughly ±10–20%.

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

### 1,000,000 vectors (128 dimensions, exact cosine)

Release daemon, ext4, `NONODB_CACHE_TTL=0`, `NONODB_MAP_SIZE=6GiB`.
Each document carried one deterministic 128-dimensional vector. Inserts used
batches of 500 through the Python client; query vectors were supplied by the
client.

| Operation | Result |
|---|---|
| Insert 1M documents with vectors | 99.35 s, **10,066 docs/s** including client JSON/UDS overhead |
| Vector index backfill | 8.08 s, **123,746 docs/s** |
| `data.mdb` after documents | 3,935 MiB |
| `data.mdb` after vector + two secondary indexes | 4,767 MiB |
| `vector_search`, no filter, limit 10 | p50 **298.9 ms**, p95 349.7 ms, p99 376.5 ms |
| `vector_search`, no filter, limit 100 | p50 **306.8 ms**, p95 346.0 ms |
| Filter by indexed `cat` (~10k matches) | p50 **118.5 ms**, p95 157.2 ms |
| Filter by indexed exact `n` (1 match) | p50 **0.82 ms**, p95 1.17 ms |
| Filter by exact `_id` | p50 **0.75 ms**, p95 1.44 ms |
| Filter by unindexed exact `title` (worst case) | **8.88 s** for one query |
| Daemon RSS after searches | ~5.6 GiB (includes resident LMDB mapped pages) |

The unfiltered cost is the expected exact scan: roughly one million vectors ×
128 components per query. The vector engine uses an exact `_id` or a secondary
index for selective filters when available; an unindexed highly selective filter
can inspect most documents and approach a full collection scan.

## Aggregation pipeline

Measured on 2026-11-10 with `scripts/aggregate_bench.py`, release build, ext4,
50,000 documents, 10 measured iterations after 2 warmups. The daemon was started
with `NONODB_CACHE_TTL=0` so the numbers measure execution rather than repeated
cache hits. Timings include HTTP/Unix-socket transport and JSON decoding in the
Python client.

Each row is a separate pipeline. `rows` is the number of documents returned by
that pipeline.

| Pipeline | Rows | p50 | p95 |
|---|---:|---:|---:|
| `$match` broad (`active: true`) | 25,000 | 371.92 ms | 413.36 ms |
| `$match` selective (`seq < 100`, no index) | 100 | 72.10 ms | 117.54 ms |
| `$project` include | 50,000 | 436.31 ms | 488.21 ms |
| `$project` computed (`$multiply`) | 50,000 | 420.40 ms | 473.28 ms |
| `$addFields` computed | 50,000 | 809.15 ms | 847.29 ms |
| `$unset` two fields | 50,000 | 554.60 ms | 604.68 ms |
| `$sort` alone | 50,000 | 886.80 ms | 952.60 ms |
| `$sort` + `$limit 100` | 100 | 73.40 ms | 89.00 ms |
| `$skip` half the collection | 25,000 | 278.16 ms | 306.71 ms |
| `$limit 100` | 100 | 1.38 ms | 2.19 ms |
| `$count` | 1 | 0.51 ms | 0.60 ms |
| `$group` by 1,000 keys, `$sum`/`$avg` | 1,000 | 172.53 ms | 201.39 ms |
| `$group` by 100 keys, `$push`/`$addToSet` | 100 | 166.05 ms | 213.90 ms |
| `$unwind` two-element arrays | 83,333 | 890.01 ms | 1,026.57 ms |
| Combined `$match` → `$group` → `$sort` → `$limit` → `$project` | 10 | 88.30 ms | 95.91 ms |
| Same report without the leading `$match` | 10 | 154.20 ms | 158.22 ms |

The early `$match` variants show the value of reducing the working set before
`$group`: the combined report is **1.75× faster** than the same report over the
whole collection.

### What the numbers mean

- **`$limit` and a lone `$count` are effectively free after pushdown.** They
  became 1.38 ms and 0.51 ms respectively, versus roughly 144 ms and 141 ms
  when the engine still materialized all 50,000 documents first.
- **`$sort` + `$limit` is much cheaper than `$sort` alone.** The bounded TopK
  path returned the top 100 in 73 ms; sorting all 50,000 documents took 887 ms.
  Add `$limit` whenever the application only needs a page of results.
- **Selective filters without an index still scan.** The 100-row `$match`
  took 72 ms because `seq` had no index. Create an index for a field used by a
  repeated leading `$match`; the query planner will then use an index range and
  avoid parsing every document.
- **`$addFields`, `$unset`, and `$unwind` are CPU/memory-bound.** They touch
  every document and, for `$unwind`, can increase the number of rows. Filter
  and limit before them whenever possible.
- **`$group` is dominated by scanning and grouping keys.** Reducing input via
  a leading `$match` is the largest single win. A leading `$group` now streams
  documents directly into accumulators; high group cardinality still consumes
  memory and is not spilled to disk. Raise `NONODB_AGGREGATE_MAX_DOCS` only
  when the host has enough RAM.
- **`$sort` followed by `$limit` uses a bounded top-K heap.** Sorting without a
  following limit still materializes and sorts the whole result.
- **Use the read cache for repeated reports.** It is disabled in this benchmark
  to show real execution cost. With the default 5-second TTL, an unchanged
  pipeline can be served from the daemon cache.

### Reproducing

```bash
NONODB_CACHE_TTL=0 NONODB_DATA=/tmp/nonodb-agg \
  NONODB_SOCK=/tmp/nonodb-agg.sock ./target/release/nonodb-daemon

NONODB_SOCK=/tmp/nonodb-agg.sock \
  python3 scripts/aggregate_bench.py --docs 50000 --iters 10 --warmup 2
```

The script keeps the data set at or below the default 100,000-document
aggregation budget (`NONODB_AGGREGATE_MAX_DOCS`). Values vary with page-cache
state, CPU frequency, and filesystem; compare rows from the same run rather
than mixing separate runs.

## Vault encryption overhead

Vault collections encrypt and decrypt the whole document with
XChaCha20-Poly1305. This is optional: standard collections do not pay this cost.

Measured with `scripts/vault_bench.py`, 5,000 documents, 256-byte payload,
1,000 read/update iterations, daemon history enabled, `NONODB_CACHE_TTL=0`,
and the database on **ext4**:

| Operation | Standard | Vault | Vault overhead |
|---|---:|---:|---:|
| Bulk insert (`insert_many`, 200/chunk) | ~7,130 docs/s | ~2,560 docs/s | **2.79x** |
| `find_one` by `_id` p50 | 0.97 ms | 1.32 ms | **1.36x** |
| `find` by `tenant_id`, limit 100 p50 | 5.51 ms | 26.71 ms | **4.85x** |
| `count` by `tenant_id` p50 | 70.07 ms | 124.47 ms | **1.78x** |
| `update` by `_id` p50 | 2.90 ms | 3.49 ms | **1.20x** |

Interpretation:

- Point reads and updates add roughly one third of latency for encryption,
  decryption, and a fresh nonce.
- Listing a tenant is more expensive because every returned document is
  decrypted. Use `limit` for wide tenant lists.
- Counting does not decrypt documents, but both profiles still perform an
  unindexed `tenant_id` scan; vault collections cannot have a secondary index.
- Bulk insert pays for serialization, encryption, base64 encoding, and the
  larger stored document.
- Vault reads are not served from the daemon cache, so repeated reads do not
  turn into cache hits.

Reproduce:

```bash
NONODB_CACHE_TTL=0 NONODB_SOCK=$S \
  python3 scripts/vault_bench.py --docs 5000 --iters 1000 --payload 256
```

These numbers are a baseline for this host. Measure your own workload, key
size, document size, and history settings.

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

### Parallel unindexed scans, raw prefilter, and `_id` ranges

Fresh 1M-document database, ~206 MB, no secondary indexes, ext4, release
build, daemon read cache disabled. The query field is deliberately unindexed.

| Operation | Result |
|---|---|
| Load 1M (`insert_many` chunks of 500) | 12.7 s (79k docs/s) |
| `find_one(name=missing)` — serial, raw prefilter | p50 **187 ms** |
| `find(name=missing, limit=100)` — parallel, raw prefilter | p50 **99 ms** |
| `find(name=user 999999, limit=100)` — parallel, raw prefilter | p50 **123 ms** |
| `find(grp=1, limit=100)` — numeric filter, no prefilter | p50 **86 ms** |
| `find(grp=1, limit=1000)` — numeric filter | p50 **264 ms** |
| `find(_id range of 1,000, limit=100)` | p50 **1.5 ms** |
| `find(_id range, sort _id desc, limit=10)` | p50 **0.7 ms** |
| `count(_id range of 1,000)` | p50 **0.9 ms** |
| `find(name=missing, sort name, limit=100)` — parallel sorted | p50 **117 ms** |

Raw prefilter speeds up string equality scans by roughly 2–4×. `_id` ranges
use the primary B+ tree and avoid the scan entirely.

### Daemon read cache

Same database, cache enabled with a 5-second TTL:

| Operation | Cold | Cached |
|---|---:|---:|
| `find(name=cache-test-xyz, limit=100)` | 208 ms | **2.2 ms** |
| `count(grp=1)` | 783 ms | **1.2 ms** |

Every committed write clears the cache and increments its version. A read that
started before the commit cannot repopulate stale data.

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

## 10M documents

Fresh database, 10M documents (~150 bytes), indexes `grp`, `age`, `score`, plus
500k text documents. `NONODB_MAP_SIZE=32GiB`, ext4, release build.

| Metric | Result |
|---|---|
| Load 10M (`insert_many` chunks of 500) | **157.8 s (63,375 docs/s)** |
| `data.mdb` after 3 indexes | 6.24 GB |
| Backfill 3 indexes | **75.5 s** |
| `find_one` by `_id` | 0.64 ms p50, 1.37 ms p95 |
| `find(grp=x, limit=10)` via index | 1.01 ms p50, 2.09 ms p95 |
| `find(age range, limit=10)` via index | 0.51 ms p50, 0.84 ms p95 |
| `count({})` | 0.49 ms p50, 0.86 ms p95 |
| `count(grp=x)` via index | 4.98 ms p50, 37.43 ms p95 |
| `find(grp=x, sort age, limit=10)` | 42.72 ms p50, 65.90 ms p95 |
| `find(name=..., limit=1)` **without index** | 1,269 ms p50, 1,762 ms p95 |
| 1,000 batched `find_one` | 24.2 ms (41,367 ops/s) |
| Load 500k text documents | 10.6 s |
| Text index backfill, 500k docs | 5.0 s |
| Fuzzy index backfill, 500k docs | 12.6 s |
| Data file after text/fuzzy indexes | 7.12 GB |

Indexed point reads remain sub-millisecond to low-millisecond at 10M documents.
Unindexed scans are linear and take over a second at this size. The synchronous
index backfill on 10M documents takes ~76 s. Index builds use the dedicated
`NONODB_INDEX_TIMEOUT` (3600 s by default), so they are not cut off by the 30 s
`NONODB_REQUEST_TIMEOUT`; the client timeout must still cover the build. Before
this split, the same run failed at the default 30 s request timeout.

## Search and aggregation across BSON types

Fresh collection with 1,000,000 typed documents and 200,000 text documents.
Each document contains string, int, double, Decimal128, long above `2^53`,
bool, date, ObjectId, UUID, binary, array, and nested values. Ten secondary
indexes were created (`grp`, `num`, `dec`, `i64`, `str`, `date`, `oid`, `uuid`,
`flag`, `nested.v`). `NONODB_MAP_SIZE=8GiB`, `NONODB_CACHE_TTL=0`,
`NONODB_REQUEST_TIMEOUT=120`, ext4, release build.

| Stage | Result |
|---|---|
| Load 1M typed documents | **69.6 s (14,363 docs/s)** |
| Build 10 indexes | **28.5 s** |
| Load 200k text documents | 4.4 s |
| Text index backfill, 200k docs | 1.7 s |
| Fuzzy index backfill, 200k docs | 4.5 s |

The typed load is slower than the plain 1M/10M loads because every document
contains several Extended JSON values and nested/array fields.

Indexed lookups by type:

| Query | p50 | p95 |
|---|---|---|
| equality, int | 0.61 ms | 1.16 ms |
| range, int | 0.98 ms | 1.79 ms |
| equality, Decimal128 | 0.70 ms | 1.37 ms |
| range, Decimal128 | 1.17 ms | 2.15 ms |
| equality, long > `2^53` | 0.76 ms | 1.23 ms |
| equality, string | 0.60 ms | 1.19 ms |
| prefix `$regex`, string index | 0.91 ms | 1.26 ms |
| equality, date | 0.73 ms | 2.00 ms |
| range, date | 0.98 ms | 1.54 ms |
| equality, ObjectId | 0.71 ms | 1.32 ms |
| equality, UUID | 0.74 ms | 1.38 ms |
| equality, bool | 0.92 ms | 1.29 ms |
| `$in` with 10 ints | 1.70 ms | 2.78 ms |

Other operators and scans:

| Query | p50 | p95 |
|---|---|---|
| contains `$regex`, no index | 934.8 ms | 976.4 ms |
| equality, binary, no index | **4,398 ms** | 4,518 ms |
| `$or` over two indexed fields | **7,014 ms** | 7,382 ms |
| null / missing field | 2.14 ms | 4.75 ms |
| `$exists: true` | 1.43 ms | 1.73 ms |
| `$all` on array | 1.20 ms | 1.99 ms |
| `$size` on array | 1.18 ms | 1.20 ms |
| `$elemMatch` on scalar array | 1.48 ms | 2.02 ms |
| `$elemMatch` on object array | 6.14 ms | 27.2 ms |
| `$type` int/long/double/decimal/string/bool/date/objectId/binData | 0.92–1.37 ms | ≤ 2.96 ms |

The `$type` queries return after finding the requested 10 matches, so they are
not full scans in this dataset. A `$type` query with no matches still scans the
collection. Array operators also benefit from early exit when a limit is set;
`count` without an index shows the real scan cost.

Sort, projection, and count:

| Operation | p50 | p95 |
|---|---|---|
| sort by indexed `num`, no filter | **1,713 ms** | 2,214 ms |
| range filter + indexed sort | **0.91 ms** | 1.43 ms |
| sort by unindexed nested field | 1,828 ms | 1,899 ms |
| filter `grp` + sort `num` | 11.4 ms | 18.2 ms |
| deep skip 500 in indexed `grp` | 12.0 ms | 18.5 ms |
| projection of 3 fields | 0.76 ms | 1.33 ms |
| `count({})` | 0.54 ms | 1.22 ms |
| `count(grp=x)` | 0.60 ms | 1.14 ms |
| `count(num range)` | 0.65 ms | 1.52 ms |
| `count(tags $all)` without index | **7,165 ms** | 7,217 ms |

`explain` confirms that a sort-only query with no filter currently uses a
collection scan, while adding a range filter with the same sort switches to an
index scan with a covered sort. Top-level `$or` across two indexed fields also
uses a collection scan instead of an index union.

Aggregation over a 100,000-document `_id` range:

| Pipeline | Result rows | p50 | p95 |
|---|---|---|---|
| `$match` range + `$count` | 1 | 21.2 ms | 25.1 ms |
| `$match` indexed `grp=7` + `$count` | 1 | 0.65 ms | 1.29 ms |
| `$project` include + `$count` | 1 | 1,958 ms | 2,095 ms |
| `$project` computed expressions + `$count` | 1 | 2,331 ms | 2,602 ms |
| `$addFields` + `$count` | 1 | 2,830 ms | 3,067 ms |
| `$unset` + `$count` | 1 | 2,070 ms | 2,198 ms |
| `$sort` + `$limit 100` | 100 | 943 ms | 1,092 ms |
| `$skip` half + `$limit 100` | 100 | 412 ms | 435 ms |
| `$limit 100` | 100 | 5.91 ms | 7.98 ms |
| `$count` | 1 | 23.6 ms | 33.9 ms |
| `$group` sum/avg/min/max/first/last/count | 1,000 | 1,981 ms | 2,185 ms |
| `$group` Decimal128 sum/avg | 1,000 | 2,272 ms | 2,507 ms |
| `$group` large-long min/max | 1,000 | 2,165 ms | 2,395 ms |
| date range `$match` + `$group` | 1,000 | 2,077 ms | 2,311 ms |
| Decimal128 range `$match` + `$count` | 1 | 5.56 ms | 7.99 ms |
| `$sort` date desc + `$limit 100` | 100 | 968 ms | 1,114 ms |
| `$group` push/addToSet | 10 | 1,811 ms | 1,951 ms |
| `$unwind` over 30k docs + `$count` | 1 | 1,454 ms | 1,529 ms |
| combined filter + group + sort + limit | 10 | 1,796 ms | 1,942 ms |
| indexed match + group | 10 | 258 ms | 309 ms |

The aggregation timings are dominated by evaluating the 100k-document
intermediate result. Decimal128 and large-integer grouping add a
small amount over the mixed numeric group, and indexed `$match` pushdown is
visible in the sub-millisecond count.

Text and fuzzy search on 200k documents:

| Operation | p50 | p95 |
|---|---|---|
| BM25 single term | 151 ms | 190 ms |
| BM25 two terms | 250 ms | 301 ms |
| BM25 quoted phrase | 254 ms | 297 ms |
| BM25 with filter | 136 ms | 157 ms |
| fuzzy distance 1 | 57.0 ms | 78.1 ms |
| fuzzy distance 2 | 72.1 ms | 145 ms |

Reproduce with:

```bash
NONODB_CACHE_TTL=0 NONODB_REQUEST_TIMEOUT=120 \
  NONODB_SOCK=/run/nonodb.sock python3 scripts/search_aggregate_bench.py
```

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

## Persistent connection scale

Sustained 10k-connection test on the same host, 200k-document collection with
an index on `grp`, `NONODB_CACHE_TTL=0`. Every client kept one HTTP connection
open and sent a mixed workload: 50% `find_one` by `_id`, 25% indexed `find`
(limit 10), 13% indexed `count`, 5% bounded aggregation, 4% insert, and 3%
`$inc` on one hot document.

| Transport | Clients | Duration | Requests | Errors | Throughput | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| UDS | 10,000 | 300 s | 1,491,594 | 0 | 4,910 req/s | 0.49 ms | 12.5 ms | 73.4 ms |
| TCP + TLS | 1,000 | 120 s | 59,940 | 0 | 483 req/s | 0.50 ms | 2.19 ms | 3.03 ms |
| TCP + TLS | 10,000 | 120 s | 582,978 | 0 | 4,148 req/s | 1.93 ms | 132 ms | 238 ms |

The UDS run held 10,013 daemon fds for five minutes, used 493–507 MB RSS and
78–84% of one CPU core. The 10k TLS run opened all connections in 17.7 s
(~564 handshakes/s), held 10,014 fds, used 334–359 MB RSS and 88–97% of one
core. Both returned to 14 fds after close with no request errors or daemon
warnings.

The first run exposed an expensive unbounded `$match(grp)` → `$group` plan.
The current code now pushes a leading `$match` through the index/query path for
order-independent groups, keeps order-sensitive accumulators on natural `_id`
order, and parallelizes full-collection groups across worker threads. The
sustained results above used the earlier bounded form; the post-fix numbers are
below.

### Post-fix aggregation and batch indexing

1M documents, 1,000 groups, ext4, `NONODB_CACHE_TTL=0`:

| Workload | Result |
|---|---|
| Full `$group`, `NONODB_AGGREGATE_WORKERS=1` | 1,488 ms p50 |
| Full `$group`, `NONODB_AGGREGATE_WORKERS=8` | 319 ms p50 (4.7x) |
| Indexed `$match(grp=123)` → `$group`, 1,000 matches | 2.55 ms p50 / 3.80 ms p95 |
| 3 indexes via `create_indexes` | 4.68 s |
| 3 indexes created sequentially | 4.94 s |

Batch indexing is only ~1.06x faster here because the single writer/commit
dominates; the avoided collection re-scans will matter more at 10M+ documents.

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
| 10k persistent UDS clients, 200k docs | ~493–507 MB | — |
| 10k persistent TLS clients, 200k docs | ~334–359 MB | — |

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
| 10M documents + 500k text | 6.7 GB → 6.7 GB | **52.4 s** | ~130 MB/s |

After restoring the 1M backup, counts, indexes, BM25, and fuzzy search all
verified correctly. After restoring the 8M backup, counts, 3 indexes, text
search, and BM25 verified correctly. The 10M backup was verified in 2m38s and
restored with `--verify` in 2m52s; both collections and all indexes were
reported intact.

## Interpreting the results

- **Indexed point/range reads scale.** 0.5–1.5 ms from 1M to 10M for the tested
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
NONODB_SOCK=$S python3 scripts/search_aggregate_bench.py
NONODB_SOCK=$S python3 scripts/compound_test.py
NONODB_SOCK=$S python3 scripts/upsert_test.py
NONODB_SOCK=$S python3 scripts/write_batch_test.py
NONODB_SOCK=$S python3 scripts/vault_regression_test.py
NONODB_CACHE_TTL=0 NONODB_SOCK=$S python3 scripts/vault_bench.py --docs 5000 --iters 1000
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
