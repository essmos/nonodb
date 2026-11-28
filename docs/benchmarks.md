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
| 100M ANN sidecar | 16 vCPU Xeon D-2141I, 31 GiB RAM | ZFS, 438 GB | `v0.28.0`, 2026-11-22 |
| 500k engine comparison | 16 vCPU Xeon D-2141I, 31 GiB RAM | ZFS, 438 GB | `v0.28.0`, 2026-11-22 |
| Graph vs Neo4j Community | 8 vCPU Haswell, 22 GiB RAM | ext4 (`/var/tmp`) | `v0.32.0`, 2026-11-24 |
| Streams vs Redis/Kafka | 4 vCPU, 14 GiB RAM | ZFS (`/var/tmp`) | `v0.34.0`, 2026-11-28 |

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

An opt-in SQ8 sidecar (`NONODB_VECTOR_ANN_QUANTIZE=sq8`) shrank the 500k/384D
sidecar from **741 MB to 193 MB**, but at this size it did not reduce latency
(about 8.3 ms p50) and recall fell from 0.9579 to 0.9543. It is therefore not
the default; it is intended for memory- or disk-constrained large collections.

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
readiness; Elasticsearch 852 s including force-merge. On disk, the NonoDB ANN
sidecar is **741 MB**; Qdrant used 854 MB (vectors only, without payload text),
and Elasticsearch 3.7 GB for body plus vector.

#### RAM and disk

| Engine | RAM (current) | Peak RAM | Disk |
|---|---:|---:|---:|
| NonoDB | 1.4 GB | 1.4 GB | 741 MB |
| Qdrant | 1.2 GB | 1.8 GB | 854 MB |
| Elasticsearch | 3.9 GB | 7.6 GB | 3.7 GB |

Qdrant's figure is the container cgroup value and includes charged page cache;
its RSS is about 440 MB. NonoDB's disk figure is the 741 MB ANN sidecar; the
LMDB documents and exact vector index are outside this vector-index comparison.
Qdrant stored vectors without payload text, while NonoDB and Elasticsearch also
stored the passage text. Elasticsearch peak memory covers bulk indexing and
force-merge.

NonoDB's practical advantage is not raw ANN latency but that vector, BM25, and
fuzzy search live in the same collection and the same single-writer database.
Hybrid ranking can combine vector candidates with BM25 or fuzzy matches without
running a separate vector service or duplicating the documents. Qdrant is
vector-only, and this Elasticsearch run measured dense-vector search only.

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

## Streams: Redis Streams and Kafka comparison

`scripts/streams_bench.py` runs the same workload against nonodb consumer
groups, Redis Streams (`XADD`/`XREADGROUP`) and Kafka 3.9 (KRaft, single node,
one partition per consumer). Same payload (159-byte JSON), same client language
(Python 3.13), same batch size (100 events per request or poll), localhost
transport.

Each run measures:

- **produce** — batched ingestion of 50k events (`insert_many` / Redis pipeline /
  `KafkaProducer`), events per second.
- **drain** — 1, 2 and 4 consumers draining the pre-filled stream, measured
  from the first delivered record so consumer start-up is excluded.
- **latency** — one event at a time through a live consumer (`insert` +
  `watch`/`XREADGROUP`/`poll`), produce→consume round trip.
- **resources** — peak RSS, CPU seconds used, block-level bytes written, on-disk
  data directory size, sampled during each phase.

### Relaxed durability

nonodb `durability: "relaxed"`, Kafka `acks=1` with the default 5 s / 1M-message
flush, Redis without persistence (`--save '' --appendonly no`).

| Backend | Produce | Drain ×1 | Drain ×2 | Drain ×4 | Round trip p50 / p99 | Peak RSS | CPU | Disk |
|---|---|---|---|---|---|---|---|---|
| nonodb streams | 14 700 ev/s | 15 500 ev/s | 31 300 ev/s | 45 500 ev/s | 5.6 / 20.7 ms | 124 MB | 30–75% | 288 MB written, 18 MB dir |
| Redis Streams 7 | 45 700 ev/s | 47 600 ev/s | 56 500 ev/s | 53 200 ev/s | 0.6 / 0.9 ms | 48 MB | 39–198% | none (persistence off) |
| Kafka 3.9 | 13 100 ev/s | 16 400 ev/s | 16 400 ev/s | 16 000 ev/s | 3.5 / 4.5 ms | 543 MB | 6–10% | 8 MB per phase, 1.5 GB log dir |

### Full durability

nonodb `durability: "full"`, Redis `appendonly yes` + `appendfsync always`,
Kafka `acks=all` + `log.flush.interval.messages=1`. 20k events per phase.

| Backend | Produce | Drain ×1 | Drain ×2 | Drain ×4 | Round trip p50 / p99 | Peak RSS | Disk written |
|---|---|---|---|---|---|---|---|
| nonodb streams | 8 500 ev/s | 17 500 ev/s | 22 000 ev/s | 30 400 ev/s | 6.5 / 20.6 ms | 63 MB | 97 MB |
| Redis Streams 7 | 27 800 ev/s | 24 200 ev/s | 31 500 ev/s | 36 000 ev/s | 1.8 / 9.7 ms | 21 MB | 5 MB (AOF) |
| Kafka 3.9 | 9 100 ev/s | 12 900 ev/s | 16 500 ev/s | 15 400 ev/s | 6.2 / 19.7 ms | 366 MB | 63 MB → 1.2 GB log |

### What the numbers mean

- **These are client-bound, not engine-bound.** One Python process saturates
  before any of the three servers does: `kafka-python` is pure Python, and the
  nonodb client pays one HTTP round trip per request. A Rust or Go producer
  reaches far higher rates on all three. Use the ratios, not the absolutes.
- **Batching is the whole story for nonodb.** A single-document `insert` costs
  about 0.6–0.9 ms (HTTP + one write transaction), which caps a naive
  one-request-per-event producer at roughly 1.2k events/s no matter how many
  client threads you add. `insert_many` with 100 documents per transaction, and
  `watch` with `limit: 100`, is what turns that into tens of thousands per
  second.
- **Drain scales with members, produce does not.** A consumer group splits the
  range across members, so 4 consumers drain ~3× faster than one. Producers on
  nonodb share the single writer, so extra producer connections do not raise
  throughput — they only raise the batch size per commit.
- **Redis wins on raw append and round-trip latency.** It is a single-threaded
  in-memory server with no per-request transaction and no fsync unless you ask
  for one. nonodb pays for a real LMDB transaction and an index per event.
- **Kafka's memory is the JVM.** 540 MB RSS is the broker, not the data; its log
  directory is dominated by segment files. Kafka wins nothing here on this
  single-node, single-partition-per-consumer setup: its consumers are capped by
  the Python client, and 4 consumers do not beat 2.
- **Durability costs throughput, but these three configurations are not
  equivalent.** Produce dropped from 14 700 to 8 500 ev/s on nonodb (−42%), from
  45 700 to 27 800 on Redis (−39%), and from 13 100 to 9 100 on Kafka (−31%) when
  going from the relaxed row to the full row. That is a sensitivity check, not a
  like-for-like durability benchmark: nonodb and Redis fsync once per commit or
  pipeline batch, while Kafka was pinned to `log.flush.interval.messages=1`, which
  flushes **per message** and is stricter than how Kafka is normally deployed
  (replication, with the OS flushing segments). Kafka's in-Kafka durability story
  is replica acknowledgement, not fsync; on a single node there is nothing to
  acknowledge to. The drops are also single runs at different event counts
  (50k relaxed, 20k durable).
- **Disk numbers for containers are incomplete.** `cgroup` I/O counters read 0
  for the containers on this host (overlayfs on ZFS), so only the data-directory
  growth is shown for Redis and Kafka. nonodb runs as a host process, so its
  block-level bytes come from `/proc/<pid>/io`.
- **Variance.** One run each on a shared 4 vCPU box; expect ±10–20%.
  The payload is small enough that JSON encoding is a real share of the cost.

Reproduce with:

```bash
cargo build --release -p nonodb-daemon
docker run -d --name bench-redis -p 6379:6379 redis:7-alpine \
    redis-server --save '' --appendonly no
# Kafka 3.9 in KRaft mode on 127.0.0.1:9092 (see the script docstring)
python3 scripts/streams_bench.py --backend nonodb --events 50000 --consumers 1,2,4
python3 scripts/streams_bench.py --backend redis  --events 50000 --consumers 1,2,4
python3 scripts/streams_bench.py --backend kafka  --events 50000 --consumers 1,2,4
```

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
- **Streams are commit-bound, not CPU-bound.** Every batched nonodb write pays a
  real transaction; the group-commit window batches concurrent requests into one
  commit. That is why `insert_many`/`watch limit` matter far more than client
  concurrency, and why producer throughput does not scale with connections.

## Graph traversal vs Neo4j Community

This is a small end-to-end comparison, not a claim about every graph workload.
Both engines ran on the same host with release binaries/containers and the same
deterministic graph: 2,000 nodes, 8,000 directed edges (a ring plus random
edges), and 20 reachable shortest-path pairs. Ingest used 1,000-edge batches.
The table is the median of three runs; timings are end-to-end request latency
after warmup, with p50/p95 over 100 samples.

Full dataset, methodology, caveats, and reproduction steps:
[Graph benchmark](graph-benchmark.md).

| Metric | NonoDB | Neo4j 5 Community | NonoDB advantage |
|---|---:|---:|---:|
| Ingest nodes + edges | **166 ms** (~48k edges/s) | 2,335 ms (~3.4k edges/s) | ~14.0× |
| Traverse depth 1, p50 / p95 | **0.72 / 1.30 ms** | 9.41 / 18.60 ms | ~13.1× p50 |
| Traverse depth 3, p50 / p95 | **1.53 / 2.84 ms** | 8.41 / 15.07 ms | ~5.5× p50 |
| Shortest path by hops, p50 / p95 | **0.78 / 1.53 ms** | 6.76 / 12.38 ms | ~8.7× p50 |
| All shortest paths, p50 / p95 | **0.73 / 1.36 ms** | 6.18 / 11.35 ms | ~8.5× p50 |
| Pattern match depth 2, p50 / p95 | **1.09 / 1.92 ms** | 5.50 / 9.85 ms | ~5.0× p50 |
| Cypher pattern depth 2, p50 / p95 | **1.48 / 2.06 ms** | 33.61 / 39.52 ms | ~22.8× p50 |
| Cypher shortest path, p50 / p95 | **0.99 / 1.45 ms** | 5.85 / 10.88 ms | ~5.9× p50 |

Key caveats: NonoDB used a Unix socket while Neo4j used HTTP/Cypher; Neo4j
returned only counts/IDs while NonoDB materialized projected documents; the graph
is small; Neo4j Community is single-node, so this is not a replication benchmark.
See [Graph benchmark](graph-benchmark.md) for the full discussion.

---

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
