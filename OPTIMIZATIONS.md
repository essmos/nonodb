# Performance work log

This page records the major performance changes made during development. The
current measurements and methodology live in
[docs/benchmarks.md](docs/benchmarks.md); implementation details are in
[docs/architecture.md](docs/architecture.md).

## Baseline

Initial release, ext4, 12 client processes, 24,000 writes and 96,000 reads:
about 3,334 writes/s, 6,833 reads/s, ~10 MB daemon RSS, and ~729 µs average
commit time.

The workload exposed four separate bottlenecks:

1. reads collected and sorted entire collections before applying `limit`;
2. `count` deserialized every document even for `count({})`;
3. updates and deletes ignored secondary indexes;
4. every write paid a separate `fsync`.

## Implemented optimizations

### Early exit for reads

`find` and `find_one` without a sort stop after `skip + limit` matches. Full
scans already iterate in `_id` order, so no extra sort is needed for that order.
For indexed reads, the planner fetches only `skip + limit` candidates when no
sort is requested.

Result: `find_one` on an unindexed field at 50k documents dropped from a full
scan to ~21 ms p50, and indexed `find_one` to <1 ms.

### O(1) and index-backed counts

- `count({})` / `count(null)` use LMDB's `stat().entries`.
- A filter that exactly matches the fields of an index is counted by iterating
  index keys without opening documents.
- Range counts combine `$gte`/`$lt` bounds and avoid materializing ids.

Result: `count({})` ~0.5–0.7 ms; a range count at 1M dropped from 1.07 s to
~5.6 ms.

### Indexed updates and deletes

`update` and `delete` use the same access paths as `find`: exact `_id`, then a
usable index, then a full scan. A non-multi operation stops at the first match.

Result: updating 500 indexed documents at 50k took ~13.5 ms; a non-indexed
single update took ~22 ms.

### Adaptive group commit

The writer batches requests into one LMDB transaction with one commit:

- 1 ms window for a short queue;
- 3 ms at 64 queued jobs;
- 5 ms at 256 queued jobs.

At 80 clients, the average batch grew to ~24.9 requests and commits dropped from
roughly 3,000 to 965, increasing throughput to ~3,856 writes/s.

### Client JSON and cache

- `orjson` when installed; stdlib `json` otherwise.
- Cache entries store serialized JSON instead of `deepcopy`-ed Python objects.
- Read-only retries after transport errors; writes are never retried.
- `max_blocking_threads` capped at 64 to limit thread stacks.

### Read batches

`POST /v1/batch` executes many reads in one HTTP request and one `RoTxn`. One
thousand `find_one` calls dropped from ~640 ms individually to ~19–27 ms in
batches of 100 — roughly 33× with `orjson`.

### Top-K and skip

Full sorts now use a bounded heap sized by `skip + limit` rather than
collecting and sorting all matches. The heap has stable tie-breaking by `_id`.
Unsorted scans skip documents without parsing them where possible.

At 1M documents: full sort 7.18 s → ~1.29 s before parallelization; deep skip
1.40 s → ~37 ms.

### Index ranges and candidates

`$gte`/`$lt` conditions are combined into one index range. Bounds are
type-aware, so a numeric range does not match strings or booleans. `null`
conditions fall back to a scan because missing fields are not in the index.

### Compound and covering indexes

- Ordered compound indexes support equality prefixes plus a range or sort on
  the trailing field.
- Unique constraints apply to the full key.
- `include` stores extra fields in the index value; an inclusive projection can
  be served without fetching the document.
- Index-only sorting streams `range`/`rev_range` and stops after `skip + limit`,
  without a Top-K heap or document fetches.

At 1M: index-only sort ~1 ms, with `skip=500` ~2.3 ms, range filter + index sort
~3.5 ms.

### Parallel full scans

For full scans with a sort and `skip + limit <= 50,000`, the main thread reads
raw chunks from one snapshot and up to 16 workers parse/filter and compute local
Top-K heaps. The coordinator merges them with stable sequence numbers.

| Database | Before | After | CPU |
|---|---|---|---|
| 1M | ~1,050 ms | 237–327 ms | 4.4–5.0 cores |
| 4M | 4,935 ms | 729–782 ms | 6.2–6.4 cores |

Deep skips stay single-threaded to bound memory.

### Parallel index backfill

Index creation reads raw chunks with one read transaction; worker threads parse
JSON and encode/tokenize entries; the writer applies results in one write
transaction. At 1M documents, three index backfills dropped from ~6.2 s to
~3.4–3.7 s.

### `sonic-rs`

Read paths parse JSON with `sonic-rs`; writes and metadata use `serde_json`.
A compatibility test compares edge-case values against `serde_json`.
Full sort at 1M after this change measured ~250–420 ms.

### Analyzer and phrases

- Unicode lowercase and diacritic folding in BM25 and fuzzy indexes.
- Quoted phrases require contiguous token sequences.
- Phrase checks and residual filters are applied before pagination.

## Results summary

At 1M documents:

| Operation | Before | After |
|---|---|---|
| Range count | 1.07 s | 5.6 ms |
| `skip=500000` | 1.40 s | 36.6 ms |
| Full sort | 7.18 s | 250–420 ms |
| Range + sort | 2.58 s | 321 ms |
| Index-only sort | — | 1–2 ms |
| 1,000 batched reads | 640 ms | ~20 ms |

## Remaining opportunities

- Raw JSON passthrough for reads without projection/sort.
- Configurable backfill workers and incremental backfill writes to lower peak
  memory.
- Lower-overhead client framing than HTTP/JSON over UDS, if transport becomes
  the bottleneck.
- CBOR or compression on disk.
- More advanced text analysis (stemming, stop words, per-index analyzers).
