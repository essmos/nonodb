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
- [ ] Latency histograms and index-build progress in metrics
- [ ] Configurable backfill/scan worker counts
- [ ] Incremental index backfill writes to reduce peak RAM
- [ ] Raw JSON passthrough for `find` without projection/sort

### P0 — security

- [ ] Per-collection tokens, or users/roles with collection scopes
- [ ] Map mTLS certificate identities to authorization scopes
- [ ] Audit log for writes and administrative operations
- [ ] CRL/OCSP or a per-client token/certificate registry

### P1 — query and update compatibility

- [ ] `$rename`, `$min`, `$max`, `$mul`, `$currentDate`, `$bit`, `$pop`
- [ ] Timezone-aware date aggregation expressions (`$dateToString`,
      `$dateTrunc`, `$dateAdd`)
- [ ] Nested projections and `$slice`
- [ ] ObjectId as `_id` (currently field values only)
- [ ] BSON Timestamp and MinKey/MaxKey
- [ ] Regular-expression values and legacy BSON value compatibility, excluding
      executable code types
- [ ] Non-finite Decimal128 values

### P1 — durability and operations

- [ ] Point-in-time recovery / WAL
- [ ] Incremental backups
- [ ] Encryption-at-rest integration guidance

### P2 — indexes, search, and storage

- [ ] Sparse/partial indexes
- [ ] Wildcard indexes
- [ ] Reindex command for text/fuzzy indexes
- [ ] Stop words, stemming, synonyms, and per-index analyzers
- [ ] Optional Tantivy integration
- [ ] CBOR/MessagePack storage with migration path
- [ ] Compression on disk
- [ ] Published Python package
- [ ] Geospatial and vector indexes (separate feature area)

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

## Rules of thumb

- Keep storage semantics in `nonodb-core`; do not implement query logic in the
  daemon.
- Maintain all indexes in the same transaction as the document write.
- Use indexes only when equivalence is provable; otherwise fall back to a scan
  and residual matcher.
- Add unit tests for core semantics and an end-to-end script for HTTP changes.
- Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and
  `cargo test` before merging.
