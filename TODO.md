# Implementation backlog

This file tracks engineering work. User-facing capabilities and deliberate
limitations are documented in [docs/roadmap.md](docs/roadmap.md).

## Done

- [x] LMDB storage, collections, JSON documents, UUID `_id`
- [x] MongoDB-like filters, projection, sort, skip/limit
- [x] Insert, insert_many, update, delete, count
- [x] Update operators: `$set`, `$unset`, `$inc`, `$push`, `$pull`,
      `$addToSet`, `$setOnInsert`, `$each`
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
- [x] Maintenance worker for TTL, history pruning, and automatic-index scheduling
- [x] Daemon, Unix socket, TCP/TLS, bearer auth, uid/gid allow-lists
- [x] Group commit, request limits/timeouts, metrics
- [x] Snapshots, online backup, restore, inspect, CLI
- [x] Python client, Docker, systemd, crash/concurrency/large-data tests

## Next

### P1 — performance and operations

- [ ] Raw JSON passthrough for `find` without projection/sort
- [ ] Configurable backfill/scan worker counts
- [ ] Incremental index backfill writes to reduce peak RAM
- [ ] Reindex command for text/fuzzy indexes
- [ ] Latency histograms and index-build progress in metrics
- [ ] Structured logs and graceful drain

### P2 — query and update surface

- [ ] `$rename`, `$min`, `$max`
- [ ] Conditional `$pull` (query predicate)
- [ ] `$mul`, `$currentDate`
- [ ] Nested projections
- [x] Aggregation subset (`$match`, `$group`, `$project`, `$sort`, `$limit`)

### P3 — security and durability

- [ ] Per-collection tokens or users/roles
- [x] Mutual TLS with client certificate verification and allow-list revocation
- [ ] Map certificate identities to authorization scopes
- [ ] CRL/OCSP or per-client token registry
- [ ] Point-in-time recovery / WAL
- [ ] Incremental backups
- [ ] Encryption-at-rest integration guidance

### P4 — search and storage

- [ ] Stop words, stemming, per-index analyzers
- [ ] Optional Tantivy integration
- [ ] CBOR/MessagePack storage with migration path
- [ ] Compression on disk
- [ ] Sparse/partial and wildcard indexes

## Known issues and caveats

- `drop_index`/`drop_text_index`/`drop_fuzzy_index` remove contents but leave
  the named LMDB sub-database registered (`MAX_DBS=256`).
- `insert_many` is a single request but is not the guaranteed all-or-nothing
  primitive; use `write_batch` for atomic groups.
- Changing `_id` through update operators is unsupported; treat `_id` as
  immutable.
- Text/fuzzy indexes must be rebuilt after analyzer changes.
- LMDB files do not shrink after deletes; use compacting backup and restore.
- `map_size` cannot be resized online.
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
