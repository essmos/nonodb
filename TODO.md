# Implementation backlog

This file tracks remaining engineering work only. Completed implementation
history lives in [CHANGELOG.md](CHANGELOG.md); user-facing capabilities and
deliberate limitations are documented in [docs/roadmap.md](docs/roadmap.md).

## P0 — before public launch

- [ ] Re-run the 10M-document text benchmark and evaluate block-max WAND for
      posting lists much larger than the current 1M-document case. Use
      `scripts/large_test.py --docs 10000000 --text-docs 1000000 --text` and
      the Mongo `$text` conformance/driver tests.
- [ ] Fix long-running `create_text_index` client UX: avoid treating the
      default client timeout as a hard DDL failure, expose build status, and
      provide an async or progress-aware path for large collections.
- [ ] Test clean-machine onboarding: anonymous `docker pull`, Docker Compose
      quickstart, prebuilt binaries, backup/restore, and `kill -9` restart.
- [ ] Add `CONTRIBUTING.md`, `SECURITY.md`, and issue templates.
- [ ] Get 3–5 external testers to follow the quickstart without help.

## P0 — operations

- [ ] Raw JSON passthrough for `find` without projection/sort: stream stored
      JSON bytes directly instead of parsing to `Value` and reserializing.

## P0 — security

- [ ] Optional CRL/OCSP integration for externally managed PKI.

## Multi-database

- [ ] Shared sharded writer pool for much higher write concurrency (future
      scaling step).
- [ ] Per-database metrics, quotas, and a per-database administrator role.

## Network and host firewall

- [ ] Add firewalld/ufw backends and per-database listener policies.

## Replication

- [ ] Replicate TLS/import/node-local settings (deliberately local today).
- [ ] Add GUI controls for cluster peer token rotation and control revision.
- [ ] Add full automatic rejoin test coverage for multi-database catalogs.
- [ ] Add optional certificate pinning.
- [ ] Hot snapshot resync while the daemon keeps serving reads.
- [ ] Certificate expiry and lag dashboards.
- [ ] Alert rules for replication lag and certificate expiry.
- [ ] Add a synchronous quorum write / zero-RPO standby option.
- [ ] Add per-collection consistency profiles.
- [ ] Design orders policy: home-region owner + sync replica.
- [ ] Design payments policy: leader per account/payment shard, quorum writes,
      idempotency keys, append-only ledger, and external reconciliation.
- [ ] Evaluate external sink adapters (PostgreSQL/Kafka/webhook) from the same
      replication stream.

## P1 — query and update compatibility

- [ ] SQL extensions: joins/subqueries, scalar expressions in grouped
      `SELECT`, `UNION`/CTE, `RETURNING`, `INSERT ... SELECT`, `UPDATE ... FROM`,
      `SUM`/`AVG`/`MIN`/`MAX(DISTINCT ...)`, and SQL benchmark coverage.
- [ ] IANA time zone identifiers for date expressions.
- [ ] Regular-expression values and legacy BSON value compatibility, excluding
      executable code types.

## P1 — event streams

- [ ] Add an end-to-end test for TTL retention of consumed event collections.

## P1 — lookup enhancements

- [ ] Serve `_id`-only projections (`fields=["_id"]` or
      `projection={"_id": 1}`) directly from the unique index without reading
      the document.
- [ ] When several unique indexes match `by`, prefer the one whose `include`
      fields cover the requested projection; otherwise choose the smallest
      suitable index.
- [ ] Benchmark a direct `get` on the encoded unique-index key instead of
      range scan + `decode_index_value`; implement only if it wins for 1, 100,
      and 1,000-key batches.
- [ ] Add `lookup_all` for non-unique indexes: array-of-arrays results,
      `limit_per_key`, total document/byte caps, per-key truncation flags,
      input-order preservation, key deduplication, and optional residual
      `filter`.
- [ ] Add `explain_lookup` with selected index, covering status, key counts,
      found/missing counts, documents read, bytes returned, and elapsed time.
      Add aggregate lookup metrics and latency histograms.
- [ ] Add soft per-request limits (`max_docs`, `max_bytes`) with an explicit
      overflow policy (`error` or `truncate`) and a continuation marker instead
      of only the all-or-nothing 64 MiB budget.
- [ ] Add client-side `lookup_chunked` helpers that split large key lists into
      bounded requests while preserving input order and missing-key alignment.
- [ ] Add a client-side `lookup_join` helper for batched foreign-key resolution
      before considering a server-side join operation.
- [ ] Optionally report `missing` input indexes (or missing counts) for
      diagnostics and retry logic.
- [ ] Validate lookup limits consistently on missing-collection and batch-read
      paths, matching the normal `lookup` validation.
- [ ] Consider a per-key cache only for immutable collections or versioned
      records; invalidation on update/delete must be proven before enabling it.
- [ ] Evaluate response compression (gzip/brotli) for large lookup payloads.
- [ ] Consider vault-collection lookup with explicit per-key encryption cost
      and authorization boundaries; keep it rejected until then.

## P1 — durability and operations

- [ ] Point-in-time recovery / WAL.
- [ ] Incremental backups.
- [ ] Encryption-at-rest integration guidance.

## P1 — tooling and administration

- [ ] Add a separate read-only `nonodb-panel` web UI for browsing collections,
      documents, filters, sort/projection, indexes, collection TTL, health,
      metrics, and `explain`. Run it outside the daemon, keep the bearer token
      server-side, bind to loopback by default, and use the existing HTTP/JSON
      API. Add an explicit, guarded write mode later with confirmations and
      audit coverage.

## P2 — GUI and operator experience

- [ ] Create/drop databases directly in the GUI.
- [ ] Daemon-side auth management APIs and GUI principals/tokens management.
- [ ] Online backup and manifest verification from the GUI.
- [ ] Simultaneous multi-daemon connection tabs.
- [ ] Per-collection metrics in the GUI.
- [ ] Cancellable, resumable large imports with progress.
- [ ] Virtualized document lists and resumable cursors for very large results.
- [ ] Optional light theme.
- [ ] Accessibility audit and expanded keyboard navigation.
- [ ] Export/import of saved views and filters without credentials.

## P2 — indexes, search, and storage

- [ ] Sparse/partial indexes.
- [ ] Wildcard indexes.
- [ ] Stop words, stemming, synonyms, and per-index analyzers.
- [ ] Optional Tantivy integration.
- [ ] CBOR/MessagePack storage with migration path.
- [ ] NonoDB-managed cold per-document compression. Filesystem compression is
      documented in [docs/volume-compression.md](docs/volume-compression.md);
      live LMDB block compression is not planned.
- [ ] Published Python package.
- [ ] Approximate vector indexes (HNSW/IVF/PQ) if brute-force scans become
      limiting.
- [ ] Advanced spherical topology/line/polygon document indexes.

## Deferred scaling work

These items are intentionally deferred; do not start them now. Revisit only
with measured evidence that one node cannot hold the dataset or that sustained
writes are capped by the single writer after vertical scaling, batching,
indexing, and profiling.

- [ ] **Database-level partitioning (database → owner node).** The first, lighter
      option to revisit for multi-tenant scale. Assign each database to one
      owner; this preserves per-database consistency and can raise aggregate
      write throughput, but it does not accelerate one hot database.
- [ ] **Full sharding of a single database.** Only consider after database-level
      partitioning is insufficient. Requires shard keys, routing, rebalancing,
      fencing, and an explicit policy for cross-shard queries, unique indexes,
      and transactions.
- **Million-transactions-per-second workloads are not a target.** The measured
  10k-client runs validate connection concurrency, not extreme write throughput.

## Known issues and caveats

- BM25 single-term queries still walk every matching posting. Block-max WAND is
  not implemented; the current optimization removes per-candidate document
  parsing and bounds ranking memory, but it does not skip posting blocks.
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
- Index covering is limited to top-level inclusive projections; nested and
  positional projections fetch the full document.
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
