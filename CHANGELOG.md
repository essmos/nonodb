# Changelog

Full release notes: https://github.com/essmos/nonodb/releases

## Unreleased

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
- Added `scripts/search_scale_bench.py` and a 10M-document search benchmark.
  On the 10M collection, BM25 takes 425 ms for one term and 3.79 s for three;
  exact 16D vector search takes 543 ms; fuzzy distance 1/2 takes about 17.4 s.

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
