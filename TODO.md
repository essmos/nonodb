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

## In progress — multi-database architecture

- [x] Database = separate LMDB environment (`<root>/<db>/data.mdb`) with its own writer.
- [x] Root layout discovery and `default` migration from the legacy single `data.mdb`.
- [x] Routing `/{database}/v1/exec` and `/{database}/v1/watch`; `/v1/exec` aliases `default`.
- [x] Global endpoints: list/create/drop databases.
- [x] Per-database store, writer, cache, auto-index, maintenance, TTL/history/events/subscriptions.
- [x] Per-database cursors and live subscriptions.
- [x] Database-aware auth scopes: `read:db/collection`, `write:db/collection`; global `admin:db` remains instance-wide.
- [x] CLI `--db`, `databases list|create|drop` (info via exec).
- [x] GUI database selector, one endpoint/port.
- [x] Backup/restore per database with manifest (`--db`).
- [x] Whole-instance online backup/restore command for all databases.
- [x] Lazy open/close and `NONODB_MAX_OPEN_DATABASES`; sharded writer pool remains future work.
- [x] Database option in official Python, Rust, Node.js, and Go clients; `NONODB_DB` support.
- [x] Multi-database, GUI, and live-stream tests wired into CI.
- [x] Feature matrix inside a non-default database: CRUD, complex filters, lookup,
      aggregation, BM25, fuzzy, vector search, TTL, history/versioning, events,
      subscriptions, jobs, and automatic indexes.
- [x] Per-request write durability (`full`/`relaxed`/`unsafe`), group-commit sync rules,
      opt-in unsafe mode, `sync` operation, CLI, docs, and crash tests.
- [x] Documentation and migration guide.
- [ ] Shared sharded writer pool for much higher write concurrency (future scaling step).
- [ ] Per-database metrics, quotas, and a per-database administrator role.

## Network and host firewall

- [x] Add a Network tab with actual listener bind/advertise details.
- [x] Add an admin `/v1/network` API and network CLI visibility.
- [x] Generate dedicated nftables rules for selected NonoDB ports and source CIDRs.
- [x] Add opt-in apply/disable controls with validation and audit records.
- [ ] Add firewalld/ufw backends and per-database listener policies.

## Cluster control plane and failover hardening

- [x] Replicate the auth registry as token hashes, scopes, principals, and
      certificate fingerprints.
- [x] Replicate backup jobs and run state; backup scheduler runs only on primary.
- [x] Replicate database catalog and update the replica source list dynamically.
- [x] Replicate encrypted vault envelopes; tenant keys remain client-side.
- [x] Add a cluster peer token and source discovery for rejoin.
- [x] Add stable seed rediscovery when the stored source is unreachable.
- [x] Add replica pause/resume/disconnect controls in the GUI, CLI, and API.
- [x] Remove stale replica acknowledgements from the GUI, CLI, and API.
- [x] Keep pairing codes visible until used, expired, or explicitly revoked.
- [x] Store role, source, vote, lease, peer token, and pairings in the hidden
      `.node` LMDB system collection instead of loose JSON files.
- [x] Add one-time 8-character replica pairing in CLI and GUI.
- [x] Gate client writes on a valid quorum lease, including startup.
- [x] Make promotion readiness require the control-plane snapshot.
- [x] Wait out the old lease before automatic election.
- [x] Hard-reset a divergent replica log in place and re-pull the source.
- [x] Add partition, control-plane, discovery, pairing, lease-gate, and vault
      replication tests.
- [ ] Replicate TLS/import/node-local settings (deliberately local today).
- [ ] Add GUI controls for cluster peer token rotation and control revision.
- [ ] Add full automatic rejoin test coverage for multi-database catalogs.

## In progress — replication foundation

Branch: `feature/replication`

Design: [docs/replication.md](docs/replication.md).

### Phase 1 — durable log and status

- [x] Add core `_replication` LMDB sub-database.
- [x] Add `ReplicationTxn`/`ReplicationChange` types with sequence, origin,
      timestamp, before/after, and transaction grouping.
- [x] Add `Store::append_replication_txn_in`, `replication_after`,
      `replication_head`, and `prune_replication_through`.
- [x] Add a core test for transaction grouping, ordering, reads, and retention.
- [x] Wire the daemon writer for normal CRUD, atomic write batches, and TTL
      expiry so changes append in the same LMDB transaction as the data.
- [x] Add durable per-database sequence allocation to the writer transaction.
- [x] Add `GET /v1/replication/status` with role, origin, enabled, and head.
- [x] Add `GET /v1/replication/changes?after=...` for log inspection.
- [x] Add `scripts/replication_test.py` for CRUD, atomic batch, delete, and SQL capture.
- [x] Capture SQL writes in the replication log by merging the core observer
      output into the writer transaction.
- [x] Vault encrypted-envelope writes, index/history/config changes are replicated
      incrementally.
- [x] Add a read-only GUI Replication tab showing role, origin, head, and recent
      transactions.
- [x] Add Prometheus replication enabled, role, origin, and head metrics.
- [x] Add applied sequence and lag to status and Prometheus metrics.
- [x] Add transport and apply-error counters and cache invalidation on apply.

### Phase 2 — primary → replica

- [x] Add pull transport with optional TLS and client mTLS.
- [x] Add CA path and client certificate/key configuration.
- [x] Add hot rotation for server TLS and replication client credentials.
- [ ] Add optional certificate pinning.
- [x] Add replication snapshot endpoint, prune endpoint, and fresh-replica
      snapshot bootstrap with recorded sequence.
- [x] Add replica apply path with idempotence `(origin, seq)`.
- [x] Preserve transaction atomicity on the replica and reject client writes.
- [x] Add persisted role state and manual promote/demote with expected epoch.
- [x] Carry the epoch in replication transactions and reject stale epochs.
- [x] Require a voter majority before promotion.
- [x] Add primary quorum leases and self-demotion on lease loss.
- [x] Add automatic candidate election after the source failure timeout.
- [x] Propagate the elected primary to every configured voter.
- [x] Add source propagation for non-voting followers.
- [x] Expose the current write endpoint, `/readyz/write`, and `Location` on `read_only`.
- [x] Add automatic snapshot resync at startup when the source log was pruned.
- [x] Add in-place hard resync for a divergent replica log.
- [x] Add role-aware `/readyz` and replication fields to status/GUI.

### Phase 3 — GUI and monitoring

- [ ] Add role banner: primary, replica, standby, fenced, bootstrapping.
- [ ] Add Replication tab with peer status, lag, sequence, certificate expiry,
      last error, and pause/resume.
- [ ] Add peer setup wizard using daemon/container certificate paths only.
- [ ] Add promote/demote, resync, and test-connection actions with confirmations
      and audit entries.
- [ ] Add lag charts and replication alerts.
- [ ] Document same-port operation and per-node GUI access.

### Phase 4 — failover and policies

- [x] Add quorum voting for promotion.
- [x] Add primary quorum leases and self-demotion on lease loss.
- [ ] Add automatic candidate election after lease expiry.
- [ ] Add primary epoch/fencing and old-primary rejection.
- [ ] Add synchronous standby option.
- [ ] Add per-collection consistency profiles.
- [ ] Design orders policy: home-region owner + sync replica.
- [ ] Design payments policy: leader per account/payment shard, quorum writes,
      idempotency keys, append-only ledger, and external reconciliation.
- [ ] Evaluate external sink adapters (PostgreSQL/Kafka/webhook) from the same
      replication stream.

## Next

### P0 — query and update compatibility

- [x] Array updates: conditional `$pull`, `$pullAll`, `$push` with
      `$position`/`$slice`/`$sort`, positional `$`, `$[]`, `$[identifier]`, and
      `arrayFilters` via update request
- [x] Atomic find-and-modify: conditional update or remove returning the old
      or new document, without a separate read request
- [x] `explain` endpoint with the selected plan, index usage, scan/range bounds,
      and estimated/actual document counts
- [x] `comment` and `max_time_ms` query options for `find`, `find_one`,
      `explain`, and cursor pages, with deadline checks in serial, parallel,
      sorted, and geo scan paths
- [x] `hint` by index name/key object or `{"$natural": 1}`, plus
      `$natural` sort order for `find`, cursors, and `explain`
- [x] `comment` and `max_time_ms` for `count` and `aggregate`, including
      pushed-down find/count and grouped-scan paths
- [x] `comment` and `max_time_ms` for `cursor_aggregate` page requests
- [x] Go and Node.js client parity for `comment`, `maxTimeMS`, and `hint` on
      find/count/aggregate helpers

### P0 — operations and durability

- [x] Reuse index/search LMDB sub-database slots so create/drop cycles cannot
      exhaust `MAX_DBS`; raise `MAX_DBS` to 1024 for legacy headroom
- [x] Online `map_size` growth (`map_info` / `resize_map`) gated against active
      transactions, with a documented offline fallback
- [x] Backup/restore verification: SHA-256 manifests, deep document/index
      checks, and an automated restore drill
- [x] Graceful drain on shutdown and structured logs
- [x] Index-build progress in metrics (`processed`/`total`/`elapsed`)
- [x] Per-operation request latency histograms in JSON and Prometheus metrics
- [x] Configurable scan/backfill/aggregate worker counts and blocking threads
- [x] Batch index creation (`create_indexes`) in one collection scan
- [x] Automatic indexes in one batch per collection; isolated index-build transactions
- [x] Index-backed and parallel order-independent `$group`
- [x] Incremental index backfill writes to reduce peak RAM
- [ ] Raw JSON passthrough for `find` without projection/sort

### P0 — security

- [x] Per-collection scopes and file-based principals
- [x] Map mTLS certificate identities to authorization scopes
- [x] Audit log for writes and administrative operations
- [x] Per-client token/certificate registry with live disable/enable
- [x] Configurable per-IP failed-login lockout and audit failure counts
- [ ] Optional CRL/OCSP integration for externally managed PKI

### P1 — query and update compatibility

- [ ] SQL extensions: joins/subqueries, scalar expressions in grouped
      `SELECT`, `UNION`/CTE, `RETURNING`, `INSERT ... SELECT`, `UPDATE ... FROM`,
      `SUM`/`AVG`/`MIN`/`MAX(DISTINCT ...)`, and SQL benchmark coverage
- [x] `$rename`, `$min`, `$max`, `$mul`, `$currentDate`, `$bit`, `$pop`
- [x] Date aggregation expressions (`$dateAdd`, `$dateSubtract`, `$dateDiff`,
      `$dateTrunc`, `$dateToString`, `$dateFromString`, `$dateFromParts`,
      `$dateToParts`) with UTC and fixed-offset time zones, custom
      `$dateFromString` formats, and ISO week fields
- [ ] IANA time zone identifiers for date expressions
- [x] Nested projections, positional `$`, and basic `$meta` projections
- [x] ObjectId as `_id` (string IDs remain supported)
- [x] MongoDB wire upserts generate ObjectId `_id` for operator, replacement, and pipeline upserts
- [x] BSON Timestamp and MinKey/MaxKey
- [ ] Regular-expression values and legacy BSON value compatibility, excluding
      executable code types
- [x] Experimental MongoDB wire handshake and BSON mapping
- [x] MongoDB wire `insert`, `find`, `getMore`, and `killCursors`
- [x] MongoDB wire `update`, `delete`, `findAndModify`, `count`, `distinct`, `aggregate`
- [x] MongoDB wire `count` skip/limit and `distinct` maxTimeMS
- [x] MongoDB wire createIndexes/listIndexes/dropIndexes with custom names and directions
- [x] MongoDB wire create/drop/listCollections/listDatabases
- [x] MongoDB wire dropDatabase
- [x] MongoDB wire arbitrary `find` sort keys with cursor paging
- [x] MongoDB wire SCRAM-SHA-256 authentication
- [x] MongoDB wire TLS
- [x] MongoDB wire SCRAM-SHA-1 authentication
- [x] MongoDB wire authSource validation and auth-file principal scopes
- [x] MongoDB wire strict option validation for unsupported driver options
- [x] MongoDB wire accepts BSON regex values as read filters and rejects stored regex values with `BadValue`
- [x] MongoDB wire aggregate cursor batching and getMore paging
- [x] MongoDB wire conformance harness against a real MongoDB server
- [x] Mongo-compatible mongoexport/mongodump export and portability checks
- [x] Static MongoDB workload portability checker with JSON manifests and heuristic source scan
- [x] MongoDB wire connection limit, handshake/read/write timeouts, and limit test
- [x] CI MongoDB conformance gate with a real MongoDB service and official drivers
- [x] MongoDB wire error-code audit: duplicate key, namespace/cursor not found, invalid options, unknown command, unauthorized
- [x] Direction-aware compound index encoding and mixed-direction sort planning
- [x] Official Node.js and Go MongoDB driver compatibility tests
- [x] MongoDB wire listCollections/listDatabases filters and nameOnly
- [x] Index union for top-level `$or` across indexed fields
- [x] Use an index for sort-only queries without a filter
- [x] `$type` index ranges for indexed scalar fields
- [x] Non-finite Decimal128 values

### P1 — event streams

- [x] Add an outbox/polling helper for change events: atomic `write_batch` of
      application data plus `events`, sortable ULID `_id`, per-consumer
      checkpoints, overlap rescan for late-committed lower ids, Python worker
      CLI, and idempotent handlers
- [ ] Add an end-to-end test for TTL retention of consumed event collections
- [x] Add a small server-side `watch` long-poll for append-only outbox event
      collections, with `after`/`next` resume tokens, type filters, batching, and
      Python/Rust/Node.js/Go clients
- [x] Add continuous NDJSON/SSE `/v1/watch` streams with heartbeats, payload
      filters/projections, truncation detection, stream metrics, and Python
      reconnect helpers
- [x] Add automatic per-collection insert/update/delete change events with
      optional field-change filters, atomic outbox writes, API/CLI/client
      controls, and real-daemon tests
- [x] Add server-side stream count thresholds with grouping and time windows
- [x] Automatic events for `find_and_modify` and TTL expiry
- [x] Add persistent conditional subscriptions over collection state with
      document filters, count conditions, default level-triggered emission,
      optional edge-triggered `once`, client-renewed leases with automatic
      expiry, SQL/write hooks, clients, CLI, docs, and real-daemon tests
- [x] Add connection-scoped live subscriptions (`mode:"live"`) with direct
      SSE/NDJSON delivery, server heartbeat, disconnect cleanup, and no
      `_events` growth
- [x] Add an embedded local GUI (`nonodb-cli gui`) with React, document CRUD,
      collection settings, indexes, SQL, map settings, metrics, local API proxy,
      CI/Docker asset builds, and an end-to-end GUI test

### P2 — GUI and operator experience

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

### P1 — lookup enhancements

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

- [x] **Optimize BM25 and Mongo `$text` ranking for large term postings.**
      A fresh 1M-text-document run with a frequent term (about 37% document
      frequency) measures p50 255 ms / p95 376 ms. Text postings store
      document length, plain queries skip
      per-candidate JSON parsing and tokenization, and ranking uses a bounded
      top-K heap instead of sorting every match. `scripts/search_test.py`,
      `scripts/search_variants_test.py`, core tests, and clippy pass.
- [ ] Re-run the 10M-document text benchmark and evaluate block-max WAND for
      posting lists that are much larger than the current 1M-document case.
      Use `scripts/large_test.py --docs 10000000 --text-docs 1000000 --text`
      and the Mongo `$text` conformance/driver tests.
- [ ] Sparse/partial indexes
- [ ] Wildcard indexes
- [x] One-click reindex for secondary, text, fuzzy, and vector indexes
      (`reindex` op, CLI, GUI, and Python client)
- [ ] Stop words, stemming, synonyms, and per-index analyzers
- [ ] Optional Tantivy integration
- [ ] CBOR/MessagePack storage with migration path
- [ ] Compression on disk
- [ ] Published Python package
- [x] Single-field GeoJSON point indexes with `$near`, `$geoWithin`, and
      `$geoIntersects` for point documents
- [x] Exact brute-force vector indexes (`vector_search`, cosine/dot/L2) with
      in-transaction maintenance, filtering, and backup verification
- [ ] Approximate vector indexes (HNSW/IVF/PQ) if brute-force scans become limiting
- [ ] Advanced spherical topology/line/polygon document indexes

## Deferred scaling work

These items are intentionally deferred; do not start them now. Revisit only
with measured evidence that one node cannot hold the dataset or that sustained
writes are capped by the single writer after vertical scaling, batching,
indexing, and profiling.

- [ ] **Database-level partitioning (database → owner node).** The first, lighter
  option to revisit for multi-tenant scale. Assign each database to one owner;
  this preserves per-database consistency and can raise aggregate write
  throughput, but it does not accelerate one hot database.
- [ ] **Full sharding of a single database.** Only consider after database-level
  partitioning is insufficient. Requires shard keys, routing, rebalancing,
  fencing, and an explicit policy for cross-shard queries, unique indexes, and
  transactions.
- **Million-transactions-per-second workloads are not a target.** The measured
  10k-client runs validate connection concurrency, not extreme write throughput.

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
