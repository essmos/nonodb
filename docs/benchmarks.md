# Benchmarks

This page keeps the large-scale results that matter for capacity planning:
the **1B dense-document test** and the **BM25, fuzzy, and exact vector search**
tests. Older microbenchmarks (CRUD, aggregation, vault, backup, connection
scale) are intentionally not repeated here; they remain available in the git
history.

These numbers are reference points, not guarantees. Storage latency, CPU,
filesystem, page cache, and client overhead all affect the result.

## Test environments

| Run | Host | Filesystem | Daemon |
|---|---|---|---|
| 1M / 10M search and 1B dense | 16 vCPU Xeon D-2141I, 31 GiB RAM | ZFS mirror, 438 GB | `v0.27.0`, 2026-11-20 |

Do not compare rows from different hosts as if they came from one controlled
run. The 100M dense comparator in the 1B table comes from an older ext4 host.

## Methodology and caveats

- Measurements are end-to-end through the daemon and Python client over a Unix
  socket, unless stated otherwise.
- Release builds only; debug builds are not representative.
- Writes use durable group commit with real `fsync`.
- Latency includes JSON, transport, and client decoding.
- Indexed lookups are effectively flat with collection size; full scans and
  exact searches grow with the number of documents or postings.
- Search timings use `limit=10` and `NONODB_CACHE_TTL=0` unless noted.
- Results from different data sets are not directly comparable.

---

## 1B documents

### Data set

The 1B run uses minimal documents with only `_id` and `n = i % 1000`. There are
no secondary indexes. This measures ingest, count, full scan, `_id` lookup, and
raw reads, not payload-heavy documents or index-build time.

| Component | Value |
|---|---|
| Documents | **1,000,000,000** |
| Data directory | ZFS `data/zd0`, 438 GB |
| Daemon | `v0.27.0` |
| Map size / timeout | `NONODB_MAP_SIZE=96GiB`, `NONODB_REQUEST_TIMEOUT=1800` |
| Final `data.mdb` | **46 GB** |

### Load, scan, and point reads

| Metric | 100M comparator (ext4) | **1B (ZFS)** |
|---|---:|---:|
| Load time | 472 s | **5,553.6 s** (1 h 32 min 34 s) |
| Load throughput | 211,845 docs/s | **180,064 docs/s** |
| `data.mdb` after load | 4.6 GB | **46 GB** |
| Daemon RSS after load | 4.8 GB | **13.7 GB** |
| `count({})` | 0.001 s | **0.002 s** |
| Full scan `count({"n": 0})` | 52.9 s | **625.6 s** |
| Scan throughput | ~1.89M docs/s | **~1.60M docs/s** |
| Matches returned by that scan | 100,000 | 1,000,000 |
| `find_one` by `_id` p50 / p95 / p99 | 0.54 / 0.58 / 0.96 ms | **0.87 / 1.27 / 1.50 ms** |
| 1,000 `_id` reads in batches of 100 | 56,900 ops/s | **55,414 ops/s** |
| 16 processes × 10,000 `_id` reads | 12,097 ops/s | **9,057 ops/s** |
| Parallel read p50 / p95 / p99 | 1.18 / 2.06 / 2.57 ms | **1.63 / 2.72 / 3.33 ms** |

The 1B data set is larger than RAM. The scan therefore reads from ZFS instead
of being fully page-cache resident, which explains the lower scan throughput
compared with the 100M run.

The daemon used 13.7 GB RSS and about 103 GB virtual size after the run. The
virtual size mostly reflects the 96 GiB LMDB map; it is not all resident heap.

### Raw read path on 1B

The 1B collection was also used to compare the `v0.27.0` raw JSON `find` path
with the normal projection path. Each iteration used a unique `comment` value
so the read cache did not turn repeated calls into cache hits. Values are 30
iterations, end-to-end through Python and the Unix socket.

| Operation | p50 | p95 |
|---|---:|---:|
| `find({}, limit=10000)` — raw JSON path | **7.13 ms** | 9.66 ms |
| `find({}, projection={_id: 1}, limit=10000)` | 21.85 ms | 31.22 ms |
| `find({"n": 0}, limit=10)` — filtered, no index | **5.47 ms** | 5.59 ms |

The raw path returns the full minimal document and is still about **3× faster**
than the projection path, which parses every document and emits only `_id`.
The filtered query stops after the first ten matches, so it is not a full-scan
measurement.

Reproduce the dense load and reads with `scripts/dense_bench.py`:

```bash
NONODB_DATA=/var/lib/nonodb-1b/data \
  NONODB_SOCK=/var/lib/nonodb-1b/nonodb.sock \
  NONODB_MAP_SIZE=96GiB NONODB_REQUEST_TIMEOUT=1800 \
  ./target/release/nonodb-daemon &

NONODB_SOCK=/var/lib/nonodb-1b/nonodb.sock NONODB_TIMEOUT=1800 \
  python3 scripts/dense_bench.py --docs 1000000000 --batch 20000
```

Use a fresh data directory. The script clears the target collection if it
already contains documents.

---

## Search benchmarks

### Data sets: 1M and 10M

Both collections use the same schema: an 8-token `body`, a `title` from 10,000
values, a 16-dimensional `vec`, and a low-cardinality `cat`. Runs used the
`NONODB_SEARCH_WORKERS=16` daemon with `NONODB_CACHE_TTL=0` on ZFS.

| Phase | 1M time | 1M `data.mdb` delta | 10M time | 10M `data.mdb` delta |
|---|---:|---:|---:|---:|
| Load documents at 5,000 per batch | **15.8 s**, 63,476 docs/s | +0.25 GB | **161.0 s**, 62,123 docs/s | +2.57 GB |
| `cat` index | 1.6 s | +0.14 GB | 17.9 s | +1.33 GB |
| BM25 `body` index | 11.0 s | +0.59 GB | 106.0 s | +5.97 GB |
| Fuzzy `title` index | 11.0 s | +0.31 GB | 86.0 s | +3.16 GB |
| 16D vector index | 0.8 s | +0.13 GB | 10.0 s | +1.36 GB |
| **Total collection** | — | **≈1.42 GB** | — | **≈14.4 GB** |

The deltas are incremental costs. The database file already contains the 1B
dense collection and other benchmark data.

### BM25

| Data set | Index build | One term | Two terms | Three terms |
|---|---:|---:|---:|---:|
| 1M docs | 11.0 s | **44.4 ms** | **175 ms** | **260 ms** |
| 10M docs | 106.0 s | **436 ms** | **1.78 s** | **2.64 s** |

Timings use `limit=10`. An indexed `cat` filter takes **82.8 ms** on 1M and
**1.23 s** on 10M. Multi-term queries stream sorted posting lists into a bounded
top-K heap and reuse ID buffers instead of materializing every hit as a `String`.

### Fuzzy search

| Data set | Index build | Distance 1 | Distance 2 |
|---|---:|---:|---:|
| 1M docs | 11.0 s | **235.88 ms** | **238.41 ms** |
| 10M docs | 86.0 s | **2.17 s** | **2.17 s** |

At both sizes, distance 1 and distance 2 have nearly the same latency because
the candidate set, not the final edit-distance check, dominates. The 10M result
is down from about **17.4 s** before the candidate merge change.

### Exact vector search

Both data sets use 16-dimensional vectors and `limit=10`.

| Operation | 1M p50 / p95 | 10M p50 / p95 |
|---|---:|---:|
| Vector index build | 0.8 s | 10.0 s |
| No filter | **51.52 / 51.52 ms** | **526.19 / 526.19 ms** |
| Indexed `cat` filter | **80.94 / 80.94 ms** | **746.95 / 746.95 ms** |

The unfiltered cost is the expected exact scan: every indexed vector is
compared with the query. Selective `_id` and secondary-index filters use the
index fast path; an unindexed highly selective filter can approach a full
collection scan. Approximate indexes such as HNSW/IVF/PQ are not implemented;
exact vector search is intended for offline or low-QPS use at these sizes.

---

## Interpreting the results

- **Point lookups scale.** `_id` reads stay under 1.5 ms p99 from 1M to 1B
  documents in these tests, while `count({})` remains O(1).
- **Scans are linear.** The 1B scan is dominated by reading 46 GB that does not
  fit in RAM.
- **Raw reads matter.** Returning stored JSON bytes avoids a parse/reserialize
  cycle and is about 3× faster than the projection path on the 1B collection.
- **BM25 scales with postings.** Streaming multi-term merges avoid materializing
  every hit. Multi-term and filtered queries still need WAND or ID-range
  sharding for larger gains.
- **Exact vector search is CPU and I/O bound.** It scans every indexed vector;
  selective filters are important, and approximate indexes are not implemented.
- **RSS is not heap.** Large RSS values after scans mostly reflect mapped LMDB
  pages.

## Reproducing

The repository includes benchmark scripts under `scripts/`.

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --release --examples
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Use a real disk for benchmark data, not tmpfs:

```bash
D=$PWD/.bench
S=$D/nonodb.sock
rm -rf "$D"
mkdir -p "$D/data"
NONODB_DATA=$D/data NONODB_SOCK=$S ./target/release/nonodb-daemon &
```

Then run the relevant script:

```bash
# 1B dense load/scan/lookup
NONODB_SOCK=$S NONODB_TIMEOUT=1800 \
  python3 scripts/dense_bench.py --docs 1000000000 --batch 20000

# 1M and 10M BM25/fuzzy/vector benchmarks
NONODB_SOCK=$S python3 scripts/search_scale_bench.py --docs 1000000 --collection search1m
NONODB_SOCK=$S python3 scripts/search_scale_bench.py --docs 10000000 --collection search10m

# Smaller functional checks
NONODB_SOCK=$S python3 scripts/search_test.py
NONODB_SOCK=$S python3 scripts/search_variants_test.py
```
