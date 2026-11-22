# Benchmarks

This page keeps the large-scale results that matter for capacity planning:
the **1B dense-document test** and the **BM25, fuzzy, exact, and IVF vector
search** tests. Older microbenchmarks (CRUD, aggregation, vault, backup, connection
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
  exact searches grow with the number of documents or postings. The IVF sidecar
  reduces but does not eliminate that growth.
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
dense collection and other benchmark data. Rebuilding the text indexes to populate
the new `df` metadata raised the file from 203.08 GB to 208.96 GB through LMDB
copy-on-write; the old pages are reusable but are not returned to the filesystem.

### Real-text check: MS MARCO

The synthetic 1M/10M rows above use an 8-token body. To test a realistic text
distribution, the official **MS MARCO passage collection** was loaded into
`msmarco` (8,841,823 passages) and a 1M-passage prefix into `msmarco1m`. Queries
are real MS MARCO training queries, sampled deterministically; timings use
`limit=10` and the same daemon configuration.

| Phase | 1M real passages | Full 8.8M real passages |
|---|---:|---:|
| Load throughput | **63,469 docs/s** | **89,010 docs/s** |
| Text index build | **156.1 s** | not completed |
| Collection size | 1,000,000 docs | 8,841,823 docs |

Real MS MARCO queries against the 1M passage index (500 samples):

| p50 | p95 | p99 | Mean | QPS |
|---:|---:|---:|---:|---:|
| **91.88 ms** | **272.65 ms** | **379.73 ms** | **111.76 ms** | **8.9** |

The full 8.8M text index was stopped after 47 minutes at 3.76M processed
documents. The build rate kept falling as the LMDB posting tree grew, so the
remaining work projected to multiple hours. That is a property of the current
per-posting LMDB layout, not of the data: the same 1M real passages index in
156 s. Real passages also make queries about 2× slower than the synthetic 1M
corpus at the same document count because they contain many more terms and
postings.

These results are the main argument for moving BM25 and fuzzy search to an
immutable, block-based index. LMDB remains the source of truth for documents,
but should not be the long-term posting store for a 10M real-text collection.

#### Internal segment prototype

`nonodb-core/src/search_segment.rs` contains a compact segment format with
sorted term blocks, delta-encoded postings, document-frequency-driven AND
planning, fast block skipping, and a lazily populated memory map. The reader has
no background threads and no owned posting cache. The standalone `segment_bench`
example was run on the same full MS MARCO corpus:

| Metric | Full 8.84M passages |
|---|---:|
| Build | **397.5 s** |
| Terms | **1,576,221** |
| Postings | **357,334,107** |
| Segment file | **1.43 GB** |
| Query p50 | **1.83 ms** |
| Query p95 | **31.99 ms** |
| Query p99 | **71.86 ms** |
| Query throughput | **145.6 qps** (single client) |

The 1M real subset gives p50 **0.64 ms**, p95 **2.94 ms**, p99 **7.52 ms**, and
**961.4 qps**. The improvement over the file-read path comes from mapping the
immutable segment once and copying directly from mapped pages instead of
issuing one `pread` per block header or posting block.

#### Exact live BM25 statistics through the daemon

A release daemon was run against a freshly loaded 1M MS MARCO prefix and the
full 8.84M collection, with segment indexes and exact live statistics:

| Metric | 1M real passages | Full 8.84M passages |
|---|---:|---:|
| Index build | not measured | **625.1 s** |
| Query p50 | **1.58 ms** | **4.60 ms** |
| Query p95 | **4.14 ms** | **29.58 ms** |
| Query p99 | **7.13 ms** | **60.83 ms** |
| Mean | **1.85 ms** | **7.66 ms** |
| Throughput | **453.8 qps** | **116.4 qps** |

The 1M run used 200 queries; the full run used 500 queries with 50 warmup
queries. Daemon latency includes fetching the full top-10 documents from LMDB,
so it is higher than the standalone segment reader above, which only resolves
document ids. On the same 8.84M index, the previous daemon without live
statistics measured p50 **4.48 ms**, p95 **29.40 ms**, and **118.9 qps**;
exact statistics are within that run-to-run noise.

Concurrent clients against the same 8.84M index (200 queries per client,
20 warmup queries):

| Clients | p50 | p95 | p99 | QPS |
|---:|---:|---:|---:|---:|
| 16 | **13.37 ms** | **91.83 ms** | **201.47 ms** | **595.2** |
| 100 | **67.59 ms** | **133.85 ms** | **171.43 ms** | **1,213.4** |

The 100-client run has a lower p99 than the 16-client run because each client
samples the same deterministic query set but the outlier tail is distributed
across more requests; p50 and throughput are the useful scaling indicators.
The segment backend is the default for newly created text indexes in the
daemon, with a durable delta, tombstone generations, and synchronous flushes.
Set `NONODB_SEGMENT_TEXT=0` to keep LMDB for new indexes. Filters, phrases, negation, exact live BM25 statistics, and
suffix/full compaction are supported; matching phrases add a score bonus, while
phrase and non-indexed filter verification still loads candidate documents from
LMDB, so their latency depends on query selectivity. The concurrent daemon and
Elasticsearch comparison is in
[Elasticsearch comparison](#elasticsearch-comparison).

#### Segment reader concurrency

`segment_bench --clients N` runs the same 500-query sample from `N` threads
against one mapped segment. The process is standalone, not the daemon, so this
isolates reader scaling from socket and writer overhead.

| Clients | p50 | p95 | p99 | Aggregate QPS |
|---:|---:|---:|---:|---:|
| 1 | **1.26 ms** | **32.88 ms** | **56.22 ms** | **175.6** |
| 4 | **1.40 ms** | **36.31 ms** | **60.31 ms** | **635.6** |
| 16 | **2.55 ms** | **74.49 ms** | **125.41 ms** | **1,248.0** |
| 64 | **11.06 ms** | **299.72 ms** | **535.20 ms** | **1,225.8** |

The server has 8 physical cores and 16 hardware threads. Aggregate throughput
saturates around 1.25k QPS at 16 clients; 64 clients add queueing latency without
more throughput. This is not yet an end-to-end daemon or Elasticsearch
comparison.

#### Daemon end-to-end concurrency

A separate daemon was started with the default segment engine and a fresh
1,000,000-passage MS MARCO collection. Each Python client opens its own Unix
socket, runs five warmup queries, then 200 queries with `limit=10`:

| Clients | p50 | p95 | p99 | Aggregate QPS |
|---:|---:|---:|---:|---:|
| 1 | **1.65 ms** | **4.31 ms** | **8.33 ms** | **419.4** |
| 16 | **10.01 ms** | **19.61 ms** | **24.36 ms** | **1,407.0** |
| 100 | **66.91 ms** | **131.67 ms** | **169.00 ms** | **1,326.2** |

This is end-to-end through the daemon and Python client, including request
serialization and client overhead. Throughput saturates near 1.3k QPS; at 100
clients p50 is dominated by queueing rather than segment search. A comparison
with Elasticsearch still needs to use the same corpus, query mix, concurrency,
and client harness.

#### Elasticsearch comparison

The same 1M-passage MS MARCO corpus and the same 200-query sample (seed 42,
`limit=10`, five warmup queries) were loaded into Elasticsearch 8.15.0 and into
the default segment backend. Elasticsearch used one shard, zero replicas, a
2 GB heap, the standard analyzer, and `match` with `operator=and`. NonoDB used
its native single-field AND search over a Unix socket; Elasticsearch was queried
over loopback HTTP.

| Clients | NonoDB p50 / p95 / QPS | Elasticsearch p50 / p95 / QPS |
|---:|---:|---:|
| 1 | **1.54 / 4.10 ms / 464.3** | **1.54 / 2.00 ms / 612.8** |
| 16 | **9.96 / 19.39 ms / 1,395.8** | **14.01 / 27.49 ms / 1,030.8** |
| 100 | **66.82 / 130.85 ms / 1,333.3** | **89.49 / 191.36 ms / 944.5** |

At one client Elasticsearch has the better tail and throughput. Under
concurrency NonoDB is faster: at 16 clients it delivers about 35% more QPS with
lower p50/p95, and at 100 clients about 41% more QPS with lower queueing
latency. These are comparison-harness numbers, not a universal ranking: the
transport, client stacks, analyzer, and single-node tuning differ.

#### Segment fuzzy

`FuzzySegmentBuilder` stores normalized values in a mapped sidecar file, so
candidate verification does not issue one read per candidate. A short-query
corpus is used here because fuzzy search is intended for names and short labels,
not long natural-language passages:

| Data set | Build | Segment + values | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|
| 85,113 short MS MARCO queries | **1.7 s** | **6.9 MB + 1.8 MB** | **0.68 ms** | **1.64 ms** | **1.80 ms** | **1,247** |
| 808,731 full MS MARCO queries | **24.3 s** | **105.8 MB + 31.9 MB** | **12.07 ms** | **21.76 ms** | **29.60 ms** | **80.4** |

Fuzzy planning probes only the rarest `3*d + 1` query trigrams. A match within
Levenshtein distance `d` can destroy at most `3*d` trigrams, so every candidate
must occur in at least one of those probes. Candidates are then verified against
all trigrams and the original string. This avoids merging every posting list
for long queries and brings the 808k full-query corpus from p50 **144.51 ms** to
**12.07 ms**.

### BM25

| Data set | Index build | One term | Two terms | Three terms |
|---|---:|---:|---:|---:|
| 1M docs | 11.0 s | **43.1 ms** | **118.7 ms** | **176.1 ms** |
| 10M docs | 106.0 s | **415.2 ms** | **1.19 s** | **1.78 s** |

Timings use `limit=10`. An indexed `cat` filter takes **88.3 ms** on 1M and
**1.28 s** on 10M. Multi-term queries stream sorted posting lists into a bounded
top-K heap and reuse ID buffers instead of materializing every hit as a `String`.
New text indexes also store per-term document frequency (`df`), so multi-term
queries no longer scan every posting list twice to compute `df`. Existing
indexes fall back to the old scan until they are rebuilt.

### Fuzzy search

| Data set | Index build | Distance 1 | Distance 2 |
|---|---:|---:|---:|
| 1M docs | 11.0 s | **233.52 ms** | **233.30 ms** |
| 10M docs | 86.0 s | **2.12 s** | **2.10 s** |

At both sizes, distance 1 and distance 2 have nearly the same latency because
the candidate set, not the final edit-distance check, dominates. The 10M result
is down from about **17.4 s** before the candidate merge change.

### Exact vector search

Both data sets use 16-dimensional vectors and `limit=10`.

| Operation | 1M p50 / p95 | 10M p50 / p95 |
|---|---:|---:|
| Vector index build | 0.8 s | 10.0 s |
| No filter | **45.89 / 45.89 ms** | **528.01 / 528.01 ms** |
| Indexed `cat` filter | **84.70 / 84.70 ms** | **777.27 / 777.27 ms** |

The unfiltered cost is the expected exact scan: every indexed vector is
compared with the query. Selective `_id` and secondary-index filters use the
index fast path; an unindexed highly selective filter can approach a full
collection scan.

### IVF vector sidecar

The optional IVF sidecar uses 512 clusters and probes 16 of them. Queries scan
the probed clusters in parallel. Both data sets use 16-dimensional vectors and
`limit=10`; latency excludes index build time.

| Operation | 1M p50 / p95 / p99 | 10M p50 / p95 / p99 |
|---|---:|---:|
| ANN sidecar build | 11.6 s | 30.5 s |
| Sidecar file | 82 MB | 820 MB |
| ANN query | **2.32 / 2.53 / 2.85 ms** | **9.84 / 12.10 / 12.24 ms** |
| Exact query | **49.87 / 50.72 / 52.67 ms** | **528.01 / 528.01 ms** |
| ANN throughput | **430 qps** | **104 qps** |

The 10M sidecar was about **60× faster at p50** than the exact scan, and the
1M sidecar about **23× faster**. A 20-query sample on 10M reached recall@10 of
**1.0** against the exact index. Recall is data-dependent, so validate with
representative queries before relying on approximate results. IVF assignment is
parallelized, cutting the 10M build from 236 s to 30.5 s and the 1M build from
38.7 s to 11.6 s.

Vector updates are stored in a small LMDB delta and merged into unfiltered ANN
queries, so a few updates do not force a 528 ms exact scan. After changing one
of the 10M vectors, ANN p50 remained **9.77 ms** (p95 **12.01 ms**). After 4,096
changed ids the query falls back to exact LMDB until the sidecar is rebuilt.
Filtered vector searches deliberately use the exact path.

### 100M ANN sidecar

The 100M collection (`search100m`, 16 dimensions) was also indexed with the
default 512 clusters and `nprobe=16`. The sidecar is **8.2 GB** and took roughly
**5–6 minutes** to build. Thirty unfiltered queries with `limit=10` measured:

| Metric | 100M result |
|---|---:|
| ANN p50 | **92.2 ms** |
| ANN p95 | **108.1 ms** |
| ANN mean | **78.7 ms** |
| ANN throughput | **12.7 qps** |
| Exact p50 (all-cluster scan) | **6.12 s** |
| Recall@10 on 20 sample queries | **1.0** |

The latency grows with vectors per probed cluster: 512 clusters leave about
195k vectors per cluster, so 16 probes scan roughly 3.1M vectors. The probe
count can be changed at restart without rebuilding the sidecar:

| nprobe | p50 | p95 | QPS | Recall@10 |
|---:|---:|---:|---:|---:|
| 16 | 92.2 ms | 108.1 ms | 12.7 | **1.0** |
| 4 | **34.4 ms** | **50.9 ms** | **40.3** | **1.0** |
| 2 | **27.4 ms** | **39.8 ms** | **48.1** | **1.0** |
| 1 | **20.9 ms** | **40.2 ms** | **57.6** | **0.75** |

`nprobe=2` is the best point on this sample: it cuts p50 by **3.4×** with no
recall loss, while `nprobe=1` loses a quarter of the exact neighbours. For very
large collections, `NONODB_VECTOR_ANN_CLUSTERS` raises the cluster cap (up to
8,192) when building the sidecar, trading build time and memory for lower query
latency. The optimal cluster/probe pair depends on the distinct-vector
distribution; validate recall after changing either setting.

### Phrase positions

Segment text format v3 appends token positions to each `.seg` file. A phrase
query verifies consecutive tokens from those positions instead of fetching
every candidate document from LMDB. The same 30 quoted-phrase queries were run
against the old v2 fallback and a rebuilt v3 index:

| Corpus | Mode | p50 | p95 | Mean |
|---|---|---:|---:|---:|
| 1M MS MARCO subset | v2 + LMDB phrase fetch | 0.61 ms | **46.06 ms** | **36.18 ms** |
| 1M MS MARCO subset | v3 positions | 0.61 ms | **7.71 ms** | **8.75 ms** |
| 8.84M MS MARCO | v2 + LMDB phrase fetch | 1.05 ms | **310.29 ms** | **336.12 ms** |
| 8.84M MS MARCO | v3 positions | 0.60 ms | **48.70 ms** | **73.07 ms** |

The 8.84M segment grew from about **1.43 GB** to **5.97 GB**, because positions
are stored for every token. The 1M rebuilt segment is about **680 MB**. This is
a deliberate trade-off for phrase verification and scoring; set
`NONODB_SEGMENT_POSITIONS=0` before building an index to keep the smaller
position-free format and use the LMDB document fallback. Legacy v2 segments
remain readable and keep working without a rebuild.

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
- **Vector search scales with the sidecar.** The exact path is CPU and I/O bound
  because it scans every vector. The optional IVF sidecar cuts 10M p50 from
  about 528 ms to under 10 ms; filtered queries and oversized deltas still use exact scans.
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

# Build the optional vector ANN sidecar at daemon startup
NONODB_BUILD_VECTOR_ANN=1 NONODB_VECTOR_ANN_PROBE=16 \
NONODB_VECTOR_ANN_COLLECTIONS=search1m,search10m nonodb-daemon

# Optional: raise the IVF cluster cap for a very large collection
NONODB_BUILD_VECTOR_ANN=1 NONODB_VECTOR_ANN_CLUSTERS=2048 \
NONODB_VECTOR_ANN_COLLECTIONS=search100m nonodb-daemon

# 1M and 10M BM25/fuzzy/vector benchmarks
NONODB_SOCK=$S python3 scripts/search_scale_bench.py --docs 1000000 --collection search1m
NONODB_SOCK=$S python3 scripts/search_scale_bench.py --docs 10000000 --collection search10m

# Real MS MARCO passages (download collection.tsv and queries.train.tsv first)
NONODB_SOCK=$S python3 scripts/load_msmarco.py collection.tsv \
  --collection msmarco1m --limit 1000000 --batch 2000
NONODB_SOCK=$S python3 scripts/msmarco_bench.py queries.train.tsv \
  --collection msmarco1m --samples 500 --limit 10

# End-to-end daemon concurrency (each client gets its own socket)
NONODB_SOCK=$S python3 scripts/msmarco_bench.py queries.train.tsv \
  --collection msmarco1mseg --samples 200 --limit 10 --clients 100

# Elasticsearch comparison harness (ES must already be running)
python3 scripts/es_bench.py --url http://127.0.0.1:9200 \
  --collection-tsv collection.tsv --queries queries.train.tsv \
  --load --samples 200 --clients 100

# Internal immutable segment prototype
cargo run --release -p nonodb-core --example segment_bench -- \
  collection.tsv /var/lib/vz/nonodb-bench-1b/segments/msmarco8m.seg \
  --rebuild --queries queries.train.tsv --samples 500 --limit 10

# Reader concurrency probe (standalone, not the daemon)
cargo run --release -p nonodb-core --example segment_bench -- \
  collection.tsv /var/lib/vz/nonodb-bench-1b/segments/msmarco8m.seg \
  --queries queries.train.tsv --samples 500 --limit 10 --clients 16

# Smaller functional checks
NONODB_SOCK=$S python3 scripts/search_test.py
NONODB_SOCK=$S python3 scripts/search_variants_test.py
```
