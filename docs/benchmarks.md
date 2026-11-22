# Benchmarks

This page keeps the large-scale results that matter for capacity planning:
the **1B dense-document test**, the **BM25 and fuzzy** tests, the **100M IVF
vector** test, and the **500k real-embedding engine comparison**. Older microbenchmarks (CRUD, aggregation, vault, backup, connection
scale) are intentionally not repeated here; they remain available in the git
history.

These numbers are reference points, not guarantees. Storage latency, CPU,
filesystem, page cache, and client overhead all affect the result.

## Test environments

| Run | Host | Filesystem | Daemon |
|---|---|---|---|
| 1B dense | 16 vCPU Xeon D-2141I, 31 GiB RAM | ZFS mirror, 438 GB | `v0.27.0`, 2026-11-20 |
| 100M ANN sidecar | 16 vCPU Xeon D-2141I, 31 GiB RAM | ZFS, 438 GB | `v0.27.0`, 2026-11-22 |
| 500k engine comparison | 16 vCPU Xeon D-2141I, 31 GiB RAM | ZFS, 438 GB | `v0.27.0`, 2026-11-22 |

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
  The daemon caches identical reads for 5 seconds by default; repeatedly timing
  the same query with the cache enabled measures a cache hit, not search.
- Results from different data sets and query mixes are not directly comparable.

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

### Real-text check: MS MARCO

The official **MS MARCO passage collection** was loaded into `msmarco`
(8,841,823 passages) and a 1M-passage prefix into `msmarco1m`. Queries are real
MS MARCO training queries, sampled deterministically; timings use `limit=10`
and the same daemon configuration.

| Phase | 1M real passages | Full 8.8M real passages |
|---|---:|---:|
| Load throughput | **63,469 docs/s** | **89,010 docs/s** |
| Collection size | 1,000,000 docs | 8,841,823 docs |

#### Internal segment implementation

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
document ids.

Concurrent clients against the same 8.84M index (200 queries per client,
20 warmup queries):

| Clients | p50 | p95 | p99 | QPS |
|---:|---:|---:|---:|---:|
| 16 | **13.37 ms** | **91.83 ms** | **201.47 ms** | **595.2** |
| 100 | **67.59 ms** | **133.85 ms** | **171.43 ms** | **1,213.4** |

The 100-client run has a lower p99 than the 16-client run because each client
samples the same deterministic query set but the outlier tail is distributed
across more requests; p50 and throughput are the useful scaling indicators.
All text indexes use the segment backend in the daemon, with a durable delta,
tombstone generations, and synchronous flushes.
Filters, phrases, negation, exact live BM25 statistics, and suffix/full
compaction are supported; matching phrases add a score bonus, while
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

A separate daemon was started with the segment engine and a fresh
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
the segment backend. Elasticsearch used one shard, zero replicas, a
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

### IVF vector sidecar (100M)

The 100M collection (`search100m`, 16 dimensions) uses the v2 ANN sidecar with
**4,096 clusters**, `nprobe=2`, and a checkpoint every 256 records. The sidecar
is **8.2 GB** and took about **21 minutes** to build. Large clusters are split
at checkpoints and scanned across all available cores, so a small `nprobe` still
uses the whole machine. A 1,000-query run with `limit=10` measured:

| Metric | ANN reader | Through the daemon |
|---|---:|---:|
| p50 | **5.35 ms** | **6.30 ms** |
| p95 | **8.63 ms** | **9.32 ms** |
| p99 | **10.36 ms** | **11.03 ms** |
| Mean | 5.69 ms | 6.64 ms |
| Throughput | 176 qps | 151 qps |

Recall@10 on 10 sample queries was **1.0** against a full sidecar scan, whose
p50 was **468 ms**. The daemon result includes fetching the ten matching
documents from LMDB. Both the p50 and p95 targets (**≤ 12 ms** and **≤ 16 ms**)
are met.

The daemon caches the memory-mapped reader per sidecar and invalidates it when
the file length or modification time changes. Mapping the 8.2 GB file for every
request discarded the process page tables for the probed ranges and added about
5 ms per query; reusing the mapping removed that cost.

The sidecar format is versioned. Version 1 files have no checkpoint metadata and
are rejected, so a sidecar built by an older release must be rebuilt once.
`NONODB_VECTOR_ANN_CLUSTERS` sets an explicit target cluster count (up to
8,192); without it the builder uses `sqrt(entries)`, capped at **4,096**.
`NONODB_VECTOR_ANN_PROBE` can change the probe count at restart without a
rebuild. The optimal cluster/probe pair depends on the
distinct-vector distribution, so validate recall after changing either setting.

### Real-embedding engine comparison (500k, 384D)

The 100M sidecar benchmark uses 16-dimensional synthetic vectors. Real
embeddings are much wider, so a separate comparison used 500,000 real MS MARCO
passages, 1,000 real MS MARCO queries, and 384-dimensional
`all-MiniLM-L6-v2` embeddings. Ground truth was exact NumPy cosine top-10 over
all vectors; every engine used a single sequential client and `limit=10`.
Qdrant and Elasticsearch ran on the same host.

| Engine | Config | p50 ms | p95 ms | p99 ms | QPS | Recall@10 |
|---|---|---:|---:|---:|---:|---:|
| NonoDB exact | no sidecar | 346.86 | 360.25 | 364.86 | 2.9 | 1.0000 |
| NonoDB ANN | 707 clusters, `nprobe=80` | 8.06 | 9.46 | 10.50 | 121.4 | 0.9579 |
| Qdrant HNSW | `m=16`, `ef_construct=100`, `hnsw_ef=32` | **2.26** | **2.64** | **2.88** | **431.5** | **0.9637** |
| Elasticsearch HNSW | `m=16`, `ef_construction=100`, `num_candidates=64` | 4.47 | 5.06 | 5.36 | 221.1 | 0.9529 |

The selected latency rows are medians of three runs; recall was identical
across repeats. At comparable recall, Qdrant was about **3.6× faster than
NonoDB ANN** at p50, and Elasticsearch about **1.8× faster**. NonoDB ANN was
about **43× faster than its own exact scan**. Raising recall increased latency only mildly for the HNSW
engines: Qdrant reached 0.9947 recall at 3.09 ms p50 (`hnsw_ef=128`), and
Elasticsearch reached 0.9781 at 5.41 ms (`num_candidates=128`).

Load and build times were: NonoDB 500k docs in 205 s plus a 15.2 s exact vector
index and 40 s for the ANN sidecar; Qdrant 114 s plus 15 s for HNSW
readiness; Elasticsearch 852 s including force-merge. On disk, NonoDB used
8.4 GB (documents, exact index, and the 741 MB ANN sidecar), Qdrant 854 MB
without payload text, and Elasticsearch 3.7 GB for body plus vector.

Storage and indexing are not perfectly equivalent: Qdrant stored only vectors,
while NonoDB and Elasticsearch also stored the passage text. Filtered vector
search is not compared because NonoDB deliberately falls back to an exact scan
when a filter is active. The harness and per-engine tuning points are in
[`scripts/vector_compare`](../scripts/vector_compare/README.md).

### Phrase positions

Segment text format v3 appends token positions to each `.seg` file. A phrase
query verifies consecutive tokens from those positions instead of fetching
every candidate document from LMDB. Thirty quoted-phrase queries were run
against the v3 indexes:

| Corpus | Mode | p50 | p95 | Mean |
|---|---|---:|---:|---:|
| 1M MS MARCO subset | v3 positions (varint term ids) | 0.62 ms | **6.30 ms** | **4.21 ms** |
| 8.84M MS MARCO | v3 positions (varint term ids) | 0.62 ms | **43.63 ms** | **37.71 ms** |

Positions are stored as varint-encoded term ordinals in a per-segment
dictionary rather than repeating token strings. The 8.84M segment is about
**3.09 GB** and the 1M rebuilt segment is about **358 MB**. Positions are
mandatory in the v3 format; there is no positions-off mode.

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
  because it scans every vector. On 100M vectors the sidecar cuts exact p50 from
  about 468 ms to **5.35 ms** at the reader and **6.30 ms** through the daemon;
  filtered queries and oversized deltas still use exact scans.
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
NONODB_DATA=$D/data NONODB_SOCK=$S NONODB_CACHE_TTL=0   ./target/release/nonodb-daemon &
```

Then run the relevant script:

```bash
# 1B dense load/scan/lookup
NONODB_SOCK=$S NONODB_TIMEOUT=1800 \
  python3 scripts/dense_bench.py --docs 1000000000 --batch 20000

# Build the optional 100M vector ANN sidecar at daemon startup
NONODB_BUILD_VECTOR_ANN=1 NONODB_VECTOR_ANN_PROBE=2 \
NONODB_VECTOR_ANN_CLUSTERS=4096 \
NONODB_VECTOR_ANN_COLLECTIONS=search100m nonodb-daemon

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

# Internal immutable segment implementation
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
