# Changelog

Full release notes: https://github.com/essmos/nonodb/releases

## Unreleased

- Memory-map immutable segment files instead of issuing `pread` calls for
  every block header and posting block. On the full 8.84M MS MARCO segment this
  moved p50 from 7.34 ms to **1.83 ms**, p95 from 235.28 ms to **31.99 ms**, and
  single-client throughput from 24.9 to **145.6 qps**.
- Added `FuzzySegmentBuilder`/`FuzzySegmentReader`: trigram postings in the
  immutable segment format with a sidecar value store, bounded candidate merge,
  Levenshtein verification, and distance-ordered results. Wired fuzzy indexes
  into the daemon behind `NONODB_SEGMENT_FUZZY=1`, including durable delta,
  tombstone generations, flushes, create/update/delete, and restart
  persistence. Filters are not supported by the segment fuzzy path yet.
- Wired the internal segment backend into the writer and daemon behind
  `NONODB_SEGMENT_TEXT=1`. New text indexes get a durable manifest, LMDB-backed
  delta, tombstone generations, synchronous 4096-document flushes, and
  create/update/delete support. The daemon search path serves simple,
  unfiltered native BM25 queries from segments. Filters, phrases, fuzzy search,
  and automatic compaction still use or require the existing backends.
- Added `SegmentSet`, an in-memory delta layer over immutable segments with
  tombstone generations. Updates replace base documents without hiding later
  flushed versions; deletes survive delta flushes. This is the write-side
  building block for the internal segment backend.
- Added the first internal immutable text-search segment backend
  (`nonodb-core/src/search_segment.rs`) and a standalone `segment_bench`
  example. It writes sorted term blocks, delta-encoded postings, and a compact
  term index without an external search engine. On the full 8.84M MS MARCO
  corpus it built in 397.5 s to a 1.43 GB segment and answered real queries at
  p50 7.34 ms / p95 235.28 ms / p99 404.37 ms with one client. Multi-term
  queries use a WAND-style upper-bound path when the rarest term is common;
  rare-term queries keep the rarest-first block-skipping path. The backend is
  not yet wired into `Collection::search`; block-max WAND is the next step for
  the p95 tail.
- Added `scripts/load_msmarco.py` and `scripts/msmarco_bench.py` and loaded the
  real MS MARCO passage collection for search validation. The 8.8M-passage load
  reached 89,010 docs/s. A 1M-passage BM25 index built in 156 s and real queries
  ran at p50 91.9 ms / p95 272.7 ms / p99 379.7 ms. The full 8.8M text index
  was stopped after 47 minutes at 3.76M documents because LMDB posting-tree
  build time kept growing; this reinforces the need for a block-based search
  sidecar such as Tantivy.
- Documented 10M, 100M, and 1B benchmark results, including the v0.27.0 raw
  `find` path on a 1B-document collection, and added `scripts/dense_bench.py`
  for reproducing the dense-document runs.
- BM25 multi-term queries now merge sorted posting-list cursors directly into
  the bounded top-K heap instead of materializing every hit as a `String`. On
  the 100M-document benchmark, two-term queries dropped from 185 s to 29.6 s and
  three-term queries from 286 s to 40.1 s.
- Text search with an exact equality filter now uses a usable secondary index as
  a prefilter before scoring. On the same 100M-document collection, a filtered
  BM25 query that previously timed out at 1800 s completed in 19.4 s.
- Single-term BM25 and unfiltered exact vector search now spread chunked work
  across `NONODB_SEARCH_WORKERS` threads and merge bounded per-worker top-K
  heaps. On the 100M-document collection, single-term BM25 dropped from 9.9 s
  to 4.2-4.4 s and exact 16D vector search from 6.7 s to 4.9-5.3 s. Multi-term
  BM25 remains on the streaming single-thread merge path.
- Fuzzy candidate selection now merges sorted trigram posting lists directly
  into a bounded 5,000-candidate heap instead of building a multi-million-entry
  `HashMap<String, u32>` and sorting every candidate. On the 10M-document
  collection, distance-1/2 queries dropped from about 17.4 s to about 2.2 s.
- Multi-term BM25 merge now reuses ID buffers and allocates a result `String`
  only when a document can enter the bounded top-K heap. New text indexes also
  store per-term document frequency, removing the extra posting-list scan used
  to compute `df`. On the 10M collection, two-term queries dropped from 2.67 s
  to 1.19 s and three-term queries from 3.57 s to 1.78 s.
- Added `scripts/search_scale_bench.py` and matching 1M/10M search benchmarks.
  On 1M, BM25 takes 46.5 ms for one term and 354 ms for three, exact 16D vector
  search takes 52.1 ms, and fuzzy distance 1/2 takes about 238 ms. On 10M the
  same operations take 454 ms, 3.57 s, 549 ms, and about 2.2 s respectively.

## v0.27.0 - 2026-11-20

### Highlights

- Plain `find` requests now use a raw JSON fast path when no projection,
  sorting, or index hint is required.
- `lookup` serves `_id`-only projections directly from the unique index.
- Long index builds expose progress through `index_status`.

### Added

- Added `CONTRIBUTING.md`, `SECURITY.md`, issue templates, and a pull request
  template.
- Added the `index_status` operation and Python `IndexBuild`/`Collection.index_status()`
  handles for observing long index backfills.
- Python `create_text_index`/`create_fuzzy_index` now default to at least a
  300-second DDL timeout and support `wait=False` for a non-blocking start.

### Changed

- Fixed the client-side default index-build timeout for `VaultDB`; vault
  `create_text_index`/`create_fuzzy_index` now use the wrapped client timeout.
- `lookup` now serves `_id`-only projections directly from the unique index;
  `fields=["_id"]` and `projection={"_id": 1}` no longer read or parse the
  stored document.
- Added a raw JSON fast path for plain `find` requests: documents without a
  projection, sort, or index hint are returned from stored bytes without a
  `Value` parse/reserialize round-trip. Filtered full scans reuse the original
  bytes after matching.
- Optimized BM25 lexical search for large posting lists. Single-term queries
  stream postings into a bounded top-K heap; multi-term queries merge sorted
  posting lists and score each document once without a shared score map.
- Benchmarked a 10M-document database with 1M text documents: p50 47–49 ms for
  one frequent term, 158 ms for two terms, and 270 ms for three terms.
- Added a 5M-text-document stress benchmark and deferred block-max WAND because
  the current direct top-K and list-merge paths are faster to ship and keep the
  release-size workload below 300 ms.

## v0.26.1 - 2026-11-19

### Highlights

- BM25 search on large posting lists is now practical: a frequent-term query
  on a 1M-text-document index measured p50 255 ms / p95 376 ms.
- Text postings store document length, plain queries skip per-candidate JSON
  parsing and tokenization, and ranking uses a bounded top-K heap.
- Added a getting started guide covering Docker Compose, prebuilt binaries,
  source builds, the first document, and core configuration.

## Completed implementation inventory

This inventory was moved out of `TODO.md` so that the backlog contains only
remaining work.

- [x] LMDB storage, collections, JSON documents, UUID `_id`
- [x] MongoDB-like filters, projection, sort, skip/limit
- [x] Insert, insert_many, update, delete, count
- [x] Update operators: `$set`, `$unset`, `$inc`, `$push`, `$pull`, `$pullAll`,
      `$addToSet`, `$setOnInsert`, `$each`, `$position`, `$slice`, `$sort`
- [x] Array update paths: numeric indexes, positional `$`, `$[]`,
      `$[identifier]`, and `arrayFilters`
- [x] Upsert with filter equality seeding and replacement upserts
- [x] Single-field, compound, unique, and covering indexes
- [x] Index-only sorted scans, range plans, index-backed count
- [x] Parallel full scans and parallel index backfill
- [x] Parallel unsorted scans with adaptive wave-based early exit
- [x] Primary-key `_id` range scans for find/count/update/delete
- [x] Raw JSON prefilter for simple string/bool equalities
- [x] Daemon-level read cache with commit invalidation
- [x] Opt-in automatic indexes for frequent read shapes with TTL cleanup
- [x] BM25 full-text search, diacritic folding, quoted phrases
- [x] Fuzzy search with trigrams and bounded Levenshtein distance
- [x] Unified multi-field search index definitions with weighted BM25
- [x] MongoDB wire `$text`: one text index, weights, phrase/negative clauses,
      `$meta: "textScore"`, find/count/distinct/leading aggregate `$match`
- [x] Read batches and atomic write batches
- [x] Ordered point lookups (`lookup`/`lookup_one`) by `_id` or a unique
      single-field/compound index, with covering projections, `_id` exclusion,
      key deduplication, response-size cap, and batch-client helpers
- [x] SQL over one collection: `SELECT` with `DISTINCT`, `WHERE`, `GROUP BY`,
      `HAVING`, aggregates including `COUNT(DISTINCT field)`, `CASE`, scalar
      functions, arithmetic, `ORDER BY`, `LIMIT`/`OFFSET`, positional
      parameters, `explain_sql`, and fast `find` path for simple selects;
      `INSERT`, `UPDATE` with document expressions, and `DELETE` through the
      single writer with per-statement atomicity; SQL CLI and shell, SQL
      metrics, slow-SQL logging, fuzz/restart/concurrency tests, all official
      clients, daemon/auth/cache/batch integration, and docs
- [x] External background jobs/schedulers through a normal `_jobs` collection:
      atomic claims, leases, retries, idempotent enqueue, Python worker helper,
      systemd unit, and docs; no daemon plugins or stored procedures
- [x] Document TTL with collection defaults, document overrides, background sweeps
- [x] BSON-compatible Extended JSON dates, indexed ranges, sorting, and unique constraints
- [x] Exact numeric comparison/indexing, Decimal128, special doubles, decimal arithmetic
- [x] Extended JSON `$numberInt`/`$numberLong`, `$type`, binary/UUID, ObjectId
- [x] `$regex`/`$options` with safe regex, compiled cache, anchored-index prefixes
- [x] Array query operators `$all`, `$size`, `$elemMatch`
- [x] Maintenance worker for TTL, per-collection history pruning, and automatic-index scheduling
- [x] Read-only aggregation subset (`$match`, `$group`, `$project`, `$sort`, `$limit`)
- [x] Daemon, Unix socket, TCP/TLS, bearer auth, uid/gid allow-lists
- [x] Mutual TLS with client certificate verification and allow-list revocation
- [x] Group commit, request limits/timeouts, metrics
- [x] Snapshots, online backup, restore, inspect, CLI
- [x] Python client, Docker, systemd, crash/concurrency/large-data tests
- [x] Planner: sort-only index scans, top-level `$or` index union, `$type` index ranges
- [x] Index-build progress in `/metrics` and a dedicated `NONODB_INDEX_TIMEOUT`
- [x] `find_cursor`/`aggregate_cursor` keyset pagination with `get_more`/`close_cursor`
- [x] Aggregation: consecutive `$match` fusion, terminal `$count` fast path, in-place stages
- [x] Aggregation: bounded top-K `$sort`+`$limit`, fused document-local stages,
      streaming leading `$group`, configurable `NONODB_AGGREGATE_MAX_DOCS`
- [x] Aggregation: `$lookup` with `let` variables, nested lookups,
      `$unionWith` with collection shorthand or a sub-pipeline,
      `$graphLookup` with depth, depth field, restrictions, and cycle detection,
      `$setWindowFields` partitions, document and range windows, ranking,
      `$shift`, `$expMovingAvg`, `$derivative`, and `$integral`,
      plus `$densify` and `$fill` numeric/date gap filling,
      and transactional `$out`/`$merge` write stages with unique-key matching,
      `let` variables, and `whenMatched` pipelines
- [x] Multikey indexes for arrays and dotted array paths, immutable-index
      metadata, indexed `$all`/`$elemMatch`, and multikey sort order
- [x] Unique multikey indexes: same-document duplicates allowed, cross-document
      duplicates rejected
