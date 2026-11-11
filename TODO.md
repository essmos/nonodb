# Implementation backlog

This file tracks engineering work. User-facing capabilities and deliberate
limitations are documented in [docs/roadmap.md](docs/roadmap.md).

## Done

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
- [x] Read batches and atomic write batches
- [x] Document TTL with collection defaults, document overrides, background sweeps
- [x] BSON-compatible Extended JSON dates, indexed ranges, sorting, and unique constraints
- [x] Exact numeric comparison/indexing, Decimal128, special doubles, decimal arithmetic
- [x] Extended JSON `$numberInt`/`$numberLong`, `$type`, binary/UUID, ObjectId
- [x] `$regex`/`$options` with safe regex, compiled cache, anchored-index prefixes
- [x] Array query operators `$all`, `$size`, `$elemMatch`
- [x] Maintenance worker for TTL, history pruning, and automatic-index scheduling
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
- [x] Multikey indexes for arrays and dotted array paths, immutable-index
      metadata, indexed `$all`/`$elemMatch`, and multikey sort order
- [x] Unique multikey indexes: same-document duplicates allowed, cross-document
      duplicates rejected

## Next

### P0 — query and update compatibility

- [x] Array updates: conditional `$pull`, `$pullAll`, `$push` with
      `$position`/`$slice`/`$sort`, positional `$`, `$[]`, `$[identifier]`, and
      `arrayFilters` via update request
- [x] Atomic find-and-modify: conditional update or remove returning the old
      or new document, without a separate read request
- [x] `explain` endpoint with the selected plan, index usage, scan/range bounds,
      and estimated/actual document counts

### P0 — operations and durability

- [x] Reuse index/search LMDB sub-database slots so create/drop cycles cannot
      exhaust `MAX_DBS`; raise `MAX_DBS` to 1024 for legacy headroom
- [x] Online `map_size` growth (`map_info` / `resize_map`) gated against active
      transactions, with a documented offline fallback
- [x] Backup/restore verification: SHA-256 manifests, deep document/index
      checks, and an automated restore drill
- [x] Graceful drain on shutdown and structured logs
- [x] Index-build progress in metrics (`processed`/`total`/`elapsed`)
- [ ] Latency histograms
- [x] Configurable scan/backfill/aggregate worker counts and blocking threads
- [x] Batch index creation (`create_indexes`) in one collection scan
- [x] Automatic indexes in one batch per collection; isolated index-build transactions
- [x] Index-backed and parallel order-independent `$group`
- [ ] Incremental index backfill writes to reduce peak RAM
- [ ] Raw JSON passthrough for `find` without projection/sort

### P0 — security

- [x] Per-collection scopes and file-based principals
- [x] Map mTLS certificate identities to authorization scopes
- [x] Audit log for writes and administrative operations
- [x] Per-client token/certificate registry with live disable/enable
- [x] Configurable per-IP failed-login lockout and audit failure counts
- [ ] Optional CRL/OCSP integration for externally managed PKI

### P1 — query and update compatibility

- [ ] `$rename`, `$min`, `$max`, `$mul`, `$currentDate`, `$bit`, `$pop`
- [ ] Timezone-aware date aggregation expressions (`$dateToString`,
      `$dateTrunc`, `$dateAdd`)
- [ ] Nested projections and `$slice`
- [ ] ObjectId as `_id` (currently field values only)
- [ ] BSON Timestamp and MinKey/MaxKey
- [ ] Regular-expression values and legacy BSON value compatibility, excluding
      executable code types
- [x] Index union for top-level `$or` across indexed fields
- [x] Use an index for sort-only queries without a filter
- [x] `$type` index ranges for indexed scalar fields
- [ ] Non-finite Decimal128 values

### P1 — durability and operations

- [ ] Point-in-time recovery / WAL
- [ ] Incremental backups
- [ ] Encryption-at-rest integration guidance

### P1 — tooling and administration

- [ ] Add a separate read-only `nonodb-panel` web UI for browsing collections,
      documents, filters, sort/projection, indexes, collection TTL, health,
      metrics, and `explain`. Run it outside the daemon, keep the bearer token
      server-side, bind to loopback by default, and use the existing HTTP/JSON
      API. Add an explicit, guarded write mode later with confirmations and
      audit coverage.

### P2 — indexes, search, and storage

- [ ] Sparse/partial indexes
- [ ] Wildcard indexes
- [ ] Reindex command for text/fuzzy indexes
- [ ] Stop words, stemming, synonyms, and per-index analyzers
- [ ] Optional Tantivy integration
- [ ] CBOR/MessagePack storage with migration path
- [ ] Compression on disk
- [ ] Published Python package
- [x] Single-field GeoJSON point indexes with `$near`, `$geoWithin`, and
      `$geoIntersects` for point documents
- [ ] Vector indexes and advanced spherical topology/line/polygon document indexes

## Known issues and caveats

- New index/search resources use a reusable slot pool, so create/drop cycles do
  not consume new LMDB sub-databases. Legacy named sub-databases created by
  older releases cannot be deregistered with the current heed API; a
  compacting backup/restore reclaims them.
- `insert_many` is a single request but is not the guaranteed all-or-nothing
  primitive; use `write_batch` for atomic groups.
- Changing `_id` through update operators is unsupported; treat `_id` as
  immutable.
- Text/fuzzy indexes must be rebuilt after analyzer changes.
- LMDB files do not shrink after deletes; use compacting backup and restore.
- `map_size` can only grow online. Shrinking the map reservation requires a
  restart; the LMDB file itself only shrinks through a compacting backup/restore.
- Projection and index covering are top-level only.
- Search is scalar-field only and has no custom analyzers.
- Indexes created before format 5 do not contain array entries; recreate them
  after upgrading if you query or sort array fields.
- Multikey compound indexes reject parallel arrays. `$size` is not index-backed.
- Aggregation groups are not spilled to disk; raise
  `NONODB_AGGREGATE_MAX_DOCS` or reduce group cardinality for very large jobs.

## Rules of thumb

- Keep storage semantics in `nonodb-core`; do not implement query logic in the
  daemon.
- Maintain all indexes in the same transaction as the document write.
- Use indexes only when equivalence is provable; otherwise fall back to a scan
  and residual matcher.
- Add unit tests for core semantics and an end-to-end script for HTTP changes.
- Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and
  `cargo test` before merging.
