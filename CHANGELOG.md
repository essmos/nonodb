# Changelog

Full release notes: https://github.com/essmos/nonodb/releases

## Unreleased

- Add experimental `profile: "columnar"` collections: general-purpose buckets
  keyed by `_id` hash, shared column codecs, locator-based point reads, update
  overlays, compaction, and full standard-collection parity for CRUD, indexes,
  TTL, history, search, graph, aggregation, SQL, replication, backup, events,
  MongoDB wire, clients, CLI, and GUI. See `docs/columnar.md`.
- Expose `get_collection_columnar` / `set_collection_columnar` and columnar
  creation settings over HTTP/socket, MongoDB `create`/`listCollections`,
  Rust/Python/Node.js/Go clients, CLI, and GUI.
- Add columnar bucket version/settings guards and migration support via
  `scripts/nonodb-migrate.py --target-profile columnar`.
- Add columnar hardening coverage for randomized differential operations,
  concurrent writers/readers, crash/rollback recovery, interrupted compaction,
  malformed buckets, unknown formats, and float bit-exact roundtrips. The write
  path uses a row-oriented staging bucket that compaction re-encodes with the
  adaptive column codecs; unsorted scans and order-independent `$group` stages
  decode document-count-balanced chunks in parallel.
- Meet the columnar release performance gates on the canonical 100k-row
  benchmark: storage `0.485×`, batch insert `0.92×`, scan `1.63×`, and
  aggregation `1.01×` versus standard, with point reads and updates inside the
  `3×`/`5×` limits. The write path serializes and normalizes batches in
  parallel, skips inapplicable index/graph/TTL hooks, uses 32-row blocks and
  prefix/front-coded columns, and stores columnar index ids once in a compact
  key trailer. `storage_stats` reads sealed-bucket headers and subtracts
  tombstones instead of materializing the collection, so it stays bounded for
  large collections. See `docs/columnar-performance.md` and the
  `columnar_soak` example.

## v0.34.0 - 2026-11-27

Stable release focused on time-series collections, columnar compression, and a
measured comparison with TimescaleDB.

- Add `profile: "timeseries"` collections with per-series/per-window buckets,
  adaptive typed-column codecs, retention, compaction, indexes, SQL,
  aggregation, events, webhooks, replication, backup, CLI, and GUI support. See
  `docs/timeseries.md`.
- Add the reproducible 100k-row comparison with TimescaleDB, including ingest,
  range scans, aggregation, storage, operational tradeoffs, and reproduction
  commands. See `docs/timescale-benchmark.md`.

- Accept the documented time-series wire aliases `get_collection_timeseries`,
  `set_collection_timeseries` (with a `timeseries` payload), and
  `retain_timeseries`, alongside the canonical snake_case operation names.
- Recover an interrupted offline compaction from `data.mdb.before-compact` (or
  `data.mdb.compacting`) when `Store::open` finds no `data.mdb`, so a crash
  during the two-step rename cannot strand the database.
- Add a time-series settings argument to Python `create_collection` and Node.js
  `createCollection`, covering the required `timeField`.

- Add compact bucket locator indexes to time-series collections. Two
  directional keys (`series + window` and `window + series`) let `find`,
  `count`, ordered reads, and aggregation iterate bucket locators instead of
  scanning the whole collection; existing collections are backfilled once on
  `Store::open` from bucket keys alone.
- Time-series `count` on `metaField` + time filters reads only bucket headers
  and the time column/deletion bitmap, and ordered `find` with `skip`/`limit`
  walks locators and decodes only the blocks it needs. `explain` reports
  `index=bucket_locator` and `buckets_total`/`buckets_pruned`.
- Store per-bucket zone maps (numeric/time min-max) and 256-byte Bloom filters
  in bucket format v3, so filters on non-key columns are rejected before
  decoding. Existing v1/v2 buckets stay readable; `reencode` upgrades them.
- Replace per-row time-series secondary index entries with per-bucket posting
  lists (value + locator + seq bitmap). The `symbol_ts_recent` index shrank by
  about 63% versus the old per-row format, while unique indexes check
  `(locator, seq)`.
- Add materialized hourly and daily OHLCV rollups in `tsr:<collection>` for
  `$group` downsampling with `$first`/`$last`/`$min`/`$max`/`$sum`. Inserts keep an existing
  rollup current, while updates, deletes, and retention invalidate it; partial boundary windows are filled from
  raw rows (or hourly rollups for daily queries). 30-day BTC/USD OHLC dropped
  from ~814 ms to ~5 ms.
- Time-series secondary indexes now drive `update`/`delete` filters as well as
  reads, unique indexes are enforced (including insert/update/upsert/reindex and
  reuse after delete), and retention removes index entries for reaped rows.
  Backfills stream bucket-by-bucket instead of materializing every row, so large
  collections no longer need one `all_docs()` vector in memory. Partial-index
  range predicates are also recognized when a query bound is at least as
  selective as the index predicate.
- Time-series `$out`/`$merge` targets write through the bucket writer, so
  indexes, events, replication, and synthetic ids stay consistent.
- Row-local time-series aggregation stages stream in batches
  (`NONODB_TS_AGG_BATCH`); a single `$group` stage streams all supported
  accumulators into one builder so downsampling does not materialize the range.
  `$sort`/`$limit` run on the group result; other blocking stages keep the
  materialized path.
- Buckets use block-column encoding (256 rows per block), so `find_by_key`
  decodes one block instead of the whole bucket. User-supplied ids use a small
  id-to-locator map instead of a collection scan. Old `NTB1`/`NTB2` payloads
  remain readable.
- Idle databases can be compacted online without restarting the daemon; active
  runtimes are left untouched until they finish, and deleted row values are
  cleared per bucket during rewrites.
- Add `Collection::insert_many_txn` with a time-series batch writer that
  resolves each open bucket once and flushes its pointer at the end of the
  transaction. A 100k-row load in 1000-row transactions improved from ~40.5k to
  ~49k rows/s; the daemon `insert_many` path and the benchmark use it.
- Speed up time-series single-row maintenance: `update`/`findAndModify` by a
  synthetic `_id` now restrict scanning to the bucket encoded in the id, and
  unsorted indexed reads stop after `skip + limit` rows instead of decoding all
  candidates. Add `/metrics` gauges for time-series rows, deleted rows, buckets,
  encoded/raw bytes, plus retention and compaction counters, documented in
  `docs/monitoring.md`.
- Keep synthetic time-series ids stable when a pending row is deleted or moved
  and the open bucket is later sealed: sealing now preserves sequence gaps as
  deleted bitmap rows instead of renumbering the surviving rows. Added a pending
  move test alongside the `findAndModify` index test.
- Run time-series retention automatically in the daemon for collections with
  `expireAfterSeconds`, via the single writer thread, and request compaction
  when deleted pages pass `NONODB_TS_COMPACT_THRESHOLD_MB`. The daemon compacts
  an idle runtime online; startup compaction remains the crash/disabled-fallback
  path. Added `compact_data_dir` in the core crate and retention/startup tests.
- Store time-series deletions as a per-bucket bitmap appended to the columnar
  payload instead of one 27-byte tombstone key per deleted row. `NTB1`
  payloads and legacy `0x03` keys remain readable and migrate into the bitmap on
  the next write, delete, retention, or seal. 10,000 deleted rows now cost about
  1.25 KB of bitmap instead of 270 KB of keys.
- Add `profile: "timeseries"` collections with bucketed columnar storage for
  IoT and event data. A bucket covers one series (`metaField`) and one time
  window; the meta value is stored once and every other field becomes a typed
  column encoded with the cheapest of delta, delta-of-delta, Gorilla XOR,
  dictionary, frame-of-reference bit packing, boolean bit packing, byte
  shuffle, and optional zlib per column. Open buckets append rows in O(1) and
  seal when full or when the window advances. `find`, `count`, `explain`,
  ordered lookups, replication, backup, and drop work. The bundled 100k-row
  benchmark stores 4.07 bytes/row, 45x smaller than raw JSON. See
  `docs/timeseries.md`.
- Add `update` and `findAndModify` update support for time-series collections:
  `$set`, `$inc`, `$mul`, `$unset`, `$rename`, update pipelines and upserts.
  Ordinary field changes rewrite the affected bucket in place and keep the
  synthetic `_id`; changing the time or meta field is a delete + insert and
  reports the new `_id` (`new_id` from `findAndModify`). Replication replays a
  move as delete + insert and pinned locators keep ids identical on replicas.
- Add non-unique secondary indexes to time-series collections. `createIndex`,
  `createIndexes`, `dropIndex`, `listIndexes`, and `reindex` backfill from
  bucket rows; inserts, deletes, updates, upserts, and replicated writes keep
  entries current, and `find`/`count`/`explain` use the planner
  (`index_used`, `access: index_scan`). Unique indexes are enforced on the
  same paths.
- Enable text, fuzzy, geo, and vector indexes on time-series collections,
  including `$search`, `$searchMeta`, `$vectorSearch`, fuzzy/vector search, and
  `$near`/`$geoWithin`. Backfills decode bucket rows, and inserts, updates,
  deletes, upserts, and replicated writes keep the search structures current.
- SQL statements (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `EXPLAIN`) now work
  on time-series collections. Selects reuse the bucket read path and planner;
  writes reuse the time-series update/insert/delete paths, so indexes and
  synthetic ids stay consistent.
- Run time-series aggregations with the full aggregation host, enabling
  `$lookup` (local/foreign and pipeline/`let`), `$graphLookup`, and
  `$unionWith` against document or time-series collections. Tests compare the
  results with the equivalent pipeline on a document collection.
- Time-series writes now feed change events, watches, persistent/live
  subscriptions, and webhooks. `_events` entries carry the decoded row and the
  new id when an update moves it; count thresholds use the normal `count` path;
  tests cover a live receiver and a webhook delivery with a `sum` threshold.
- Add `get_collection_timeseries`/`set_collection_timeseries` (native API and
  core `Store::timeseries_settings`) with validation and replication. Structural
  fields (`timeField`, `metaField`, `granularity`) can only change on an empty
  collection. The MongoDB wire `create` command now accepts the standard
  `timeseries` object.
- GUI: the New collection dialog supports the `timeseries` profile (required
  time field plus meta field, granularity, compression, and expiry), and the
  collection settings panel shows bucket statistics, deleted rows, and
  compression sizes. Compression/bucket/expiry settings are editable; the
  structural fields are read-only once rows exist.
- CLI: add `create-collection` (including `--profile timeseries` with
  `--time-field`, meta field, granularity, compression, and expiry) and
  `get-collection-timeseries`/`set-collection-timeseries`, which read and merge
  settings through the daemon socket.
- Add aggregation and retention for time-series collections. `aggregate` now
  decodes buckets through the time-range-pruned find path and runs the normal
  in-memory pipeline, so `$group`/`$bucket` downsampling works. The new
  `retain_timeseries` write op (replicated as `time_series_retain`) drops whole
  old buckets by key without decoding columns and only rewrites the boundary
  bucket. Add `examples/timeseries_bench.rs` with write/read/aggregate/retention
  latency percentiles and a standard-collection baseline.
- Route replicated time-series inserts through the bucket writer instead of the
  unused `coll:<name>` database, and replicate `retain_timeseries` as
  `time_series_retain`. Replicas now produce byte-identical documents, including
  synthetic `_id`s, which is covered by a two-store replication test. Add
  `examples/timeseries_space.rs`: 100k rows occupy 1.02 MB compacted (10.69
  B/row) versus 41.59 MB (436.1 B/row) for a document-per-row collection, with
  the LMDB high-water caveat documented. The benchmark now also measures
  secondary-index creation/lookup, `$set` updates, SQL `GROUP BY`, bitmap
  deletes, and compaction.
- Add `delete` and `findAndModify(remove: true)` for time-series collections.
  Sealed buckets never renumber rows, so synthetic `_id`s stay valid; deleted
  rows are tracked in a per-bucket bitmap and a pure `_id` delete goes straight
  to the row locator. `find`, `count`, `find_by_key`, aggregation, and
  `verify_data_dir` skip deleted rows; `timeseries_stats` reports
  `deleted_rows`. Retention marks the boundary bucket instead of renumbering.
  Add `nonodb-cli compact <data-dir>` for offline in-place compaction
  (compacting copy + fsync + swap, with a `.before-compact` fallback), plus
  `Store::close()` so offline maintenance can reopen the environment in the same
  process.

## v0.33.0 - 2026-11-26

Stable release focused on graph traversal, read-only Cypher, and background
graph analytics.

- Add read-only Cypher execution through `cypher` and `Collection.cypher()`,
  including `MATCH`/`WHERE`/`WITH`/`UNWIND`, variable-length paths, shortest
  paths, optional matches, existential subqueries, and a bounded execution
  model. See `docs/cypher.md`.
- Add an openCypher TCK harness and CI regression gate. The pinned read-only
  baseline is 859/874 passing, with unsupported and known-gap cases reported
  explicitly.
- Add GDS-style procedures: `gds.pagerank.stream`, `gds.degree.stream`,
  `gds.connectedComponents.stream`, `gds.nodeSimilarity.stream`,
  `gds.labelPropagation.stream`, `gds.kCore.stream`, and
  `gds.degreeDistribution.stream`.
- Expand `graph_algorithm` with label propagation, k-core, and degree
  distribution; remove the hidden 1,000-node GDS cap and add configurable
  `max_nodes`, `max_edges`, `max_memory_bytes`, and `timeout_ms` budgets with
  explicit overrun errors.
- Add bounded background graph analytics jobs through `start_graph_job`,
  `graph_job_status`, `graph_job_result`, `cancel_graph_job`, and
  `export_graph_job`. Jobs support progress, cooperative cancellation,
  owner/database isolation, two concurrent runs per database, and atomic export
  with deterministic ids. Completed results are capped at 64 MiB each and
  128 MiB total; failed exports restore the result for retry.
- Add Python client helpers for Cypher, the new graph algorithms, background
  jobs, and result export.
- Add the reproducible 100k/500k/1M-node graph analytics benchmark and document
  its methodology, timings, and memory use.
- Fix graph metadata cleanup on `drop_collection`, enable the `$graphLookup`
  adjacency fast path, cap rebuild generations, and tighten Cypher string
  literal, operation, timeout, and memory-budget handling.
- Fix quadratic bulk deletion for collections with text or fuzzy indexes by
  batching tombstone writes into a single manifest update per index kind. A
  100k-document delete that previously timed out now completes in about 1.2 s.

## v0.32.0 - 2026-11-23

- Add graph search parity features: `all_shortest_paths` (BFS and weighted
  Dijkstra), `graph_match` for linear variable-length patterns with per-step
  filters, and `graph_algorithm` for bounded PageRank, degree centrality, and
  weakly connected components. Python client methods and the graph benchmark
  cover the new operations. See `docs/graph.md`.
- **Upgrade: plain indexes now index a missing field as `null`.** Run
  `nonodb-cli reindex <collection>` for every collection with plain indexes
  after upgrading. Existing plain unique indexes can fail to rebuild if several
  documents already share a missing/`null` key; resolve those duplicates first.
  Sparse and partial indexes keep their stored definitions.

- Store BSON regular-expression values (`$regularExpression`) as document data.
  They round-trip through the MongoDB wire listener, support `$type: "regex"`
  and exact `$eq` matching, and are deliberately excluded from secondary
  indexes. Malformed values return `invalid_regex`.

- Add client-side `lookup_join` helpers to the Python, Node.js, Rust, and Go
  clients. They attach matching foreign documents through chunked `lookup`,
  preserve local order and length, and align missing keys with null. See
  `docs/clients.md`.
- Add verified restore-in-place through the admin HTTP API and GUI. Preflight
  validates the backup manifest, checksum, decryption/decompression, and LMDB
  contents without changing live data; restore requires the exact
  `RESTORE <database>` phrase and records the operation in the audit log. The
  previous database is kept in `restore-safety/<database>-<timestamp>-<pid>`
  with `restore-info.json`, failures roll back automatically, and the response
  includes rollback instructions. Only non-default databases can be restored
  online; the default database still uses the offline CLI. See
  `docs/backup.md`.
- Add per-database storage quotas for data, indexes, history, and search/vector
  sidecars. `get_quota`/`set_quota`, `database_stats`, and
  `nonodb-cli quota` expose usage and limits; limits persist across restarts and
  can default from `NONODB_QUOTA_*`. Usage at 80% produces a soft warning, and
  growing writes that would cross a hard limit fail with `quota_exceeded`
  (HTTP 507) while deletes, drops, and pruning remain available. See
  `docs/quotas.md`.
- Add real sparse and partial index semantics. `sparse` now omits documents
  without the key instead of behaving like a plain index, compound sparse
  indexes skip only documents with every key absent, and
  `partialFilterExpression` limits index membership. Plain indexes now index a
  missing field as `null`, matching MongoDB, so a plain unique index allows only
  one missing/`null` key. Partial indexes are used by the planner only when the
  query repeats the predicate; hints that omit it fail instead of returning
  incomplete results. MongoDB-wire index DDL, `listIndexes`, and
  `scripts/nonodb-migrate.py` preserve both options.
- Add CRL and OCSP revocation checks for mutual TLS. CRL files/URLs refresh on a
  timer, OCSP answers are cached per certificate, and
  `NONODB_TLS_REVOCATION_FAIL_MODE` selects fail-closed (default) or fail-open
  when no source can decide. Revoked client certificates are rejected before
  registry authorization; metrics and docs cover refresh failures, revoked
  clients, and unavailable checks.
- Add opt-in point-in-time recovery through `NONODB_PITR_ENABLED=1`: periodic
  snapshots plus segmented WAL with replay to an RFC 3339/Unix timestamp during
  `nonodb-cli restore --to`. Retention prunes only WAL covered by a verified
  snapshot, hard size/age limits reject writes with `wal_full`, and per-database
  WAL metrics plus alerts report bytes, age, sequence, availability, and pruned
  bytes. See `docs/pitr.md`.

## v0.31.0 - 2026-11-23

- Add graph traversal over ordinary node and edge documents. A graph definition
  in `meta` drives native `out`/`in` adjacency slots maintained in the same
  transaction as every edge insert, update, delete, TTL expiry, and replication
  apply. Add `set_graph`, `get_graph`, `drop_graph`, `reindex_graph`
  (synchronous or online with generation swap and progress/cancel), `traverse`
  (BFS/DFS, directions, label/edge/node filters, `as_of`, projection, cycle
  markers, tree/flat output, node/edge/byte budgets, vector-search seeds),
  `neighbors`, `hybrid_search` vector+graph rerank, `shortest_path` (BFS or
  Dijkstra), adjacency-backed `$graphLookup`, Python client methods, and a lazy
  Graph tree in the GUI. Adjacency values store a compact summary instead of
  duplicating edge JSON; graph reads require edge-collection read scope and
  vault edge collections are rejected. See `docs/graph.md`.
- Remove orphaned `.dropped-*` database directories at startup. A failed
  `remove_dir_all` no longer turns an already-committed database drop into an
  error.
- Add aggregation `$convert` with `input`, `to`, `onNull`, and `onError`, plus
  `$toString`, `$toInt`, `$toLong`, `$toDouble`, `$toDecimal`, `$toBool`, and
  `$toDate`. `$lookup` pipelines using `let` and these expressions are covered.
- Add date-field TTL through `set_collection_ttl_after` and idempotent
  `backfill_collection_ttl` pages. MongoDB `createIndexes` with
  `expireAfterSeconds` and `scripts/nonodb-migrate.py` now map the indexed date
  field, backfill existing documents, and preserve the mode across restarts.
  Backfilled per-document expiry is replicated so replicas hide expired
  documents before any local sweep. Missing or non-date fields never expire;
  explicit `_ttl` still overrides.
- Add public `GET /v1/capabilities` with a versioned schema, feature flags, and
  limits. Expose it through `nonodb-cli capabilities` and the Python, Node.js,
  Rust, and Go clients.

## v0.30.0 - 2026-11-23

- Enforce TTL at read time by default. Expired documents are hidden from find,
  count, aggregation, SQL, lookups, cursors, search, and MongoDB-wire reads
  immediately at `_ttl_at`. Search paths over-fetch to fill limits, the read
  cache is capped at the next TTL deadline, and replicated documents carry
  their TTL metadata. `NONODB_TTL_STRICT_READS=0` restores legacy visibility.
- Preserve automatic collection delete events for TTL expiry when strict TTL
  reads hide the document before the sweeper runs.
- Turn the GUI SQL console into a **Playground** with SQL and JSON modes. JSON
  mode runs any `/v1/exec` request, offers find/aggregate/search/SQL/insert
  templates, formats requests, renders array results as tables, and keeps
  separate SQL/JSON history.
- Add unified `search_all` with lexical, fuzzy, vector, and hybrid modes,
  reciprocal-rank fusion, filters, pagination, and `_score`/`_sources`
  metadata. Add a `$search`/`$searchMeta`/`$vectorSearch` compatibility subset
  to aggregation pipelines plus Python/Node.js/Rust/Go/CLI helpers and Atlas
  migration documentation.
- Add native asynchronous webhooks: filtered HTTP delivery with `count`, `sum`,
  and `avg` thresholds, edge-triggered `once`, cooldown, HMAC-SHA256 signing,
  idempotency keys, durable retries/dead-letter, API/CLI management, and a GUI
  for rules and deliveries. Filters and aggregates are evaluated outside the
  writer; the writer only appends the normal change event.
- Sort missing and `null` values first, matching MongoDB's BSON ordering for
  both ascending and descending sorts. This fixes different `skip`/`limit`
  pages after a MongoDB migration.
- Resolve `$in` on `_id` and indexed fields through direct primary-key gets or
  an index range union instead of a full collection scan. Cursor pagination
  preserves the indexed path for `$in` filters.
- Commit MongoDB wire `insert_many` batches in one transaction. Ordered and
  unordered partial-success behavior is preserved when a duplicate forces the
  fallback path.
- Translate MongoDB `createIndexes` `expireAfterSeconds` into insertion-based
  collection TTL and accept `sparse` as a compatibility no-op for unique
  indexes.
- Add `scripts/nonodb-migrate.py` for quiesced MongoDB-to-NonoDB migration with
  index/TTL discovery, Extended JSON conversion, batched inserts, checksums, and
  a JSON report.
- Add an opt-in MongoDB wire shadow mode (`NONODB_MONGO_SHADOW_URL`) that mirrors
  successful writes to MongoDB and logs reply differences as JSONL. This is a
  prototype, not a production proxy.
- Add `nonodb-cli restore --to <timestamp>` for selecting the newest periodic
  snapshot at or before a recovery time, with explicit no-WAL-PITR semantics.
- Add offline `nonodb-cli branch` for independent CI/test database clones. It
  uses Linux FICLONE or macOS clonefile when available, falls back to a full
  copy, preserves search/vector sidecars, rebuilds missing sidecars from
  data-only snapshots, and supports `--force`, `--verify`, and `--no-sidecars`.

## v0.29.1 - 2026-11-22

- Accept BSON int32/int64 `_ttl` values through the MongoDB wire listener by
  parsing canonical Extended JSON integers in the TTL directive.
- Validate filters inside `$facet`, `$lookup`, `$unionWith`, `$merge`
  `whenMatched`, and `$graphLookup` sub-pipelines, so unknown operators fail
  instead of silently returning empty results.
- Update the Node.js MongoDB driver test for single-field text indexes.
- Update the replication control-plane test for the controlled exit-75 restart
  after `replication join`, so auth-registry replication is checked after the
  replica bootstraps.

## v0.29.0 - 2026-11-22

- Add `AsyncNonoDB` to the Python client for async `watch_once`, `watch`,
  `watch_stream`, and `watch_live` consumption, and document its use with
  FastAPI and other asyncio applications.
- Reject unknown filter operators in find/count/update/delete and inside `$expr`
  with `BadValue` instead of silently matching nothing. This closes the
  `$nor`/`$not` data-loss path where a typo could match every document.
- Accept MongoDB's canonical single-expression `$size` form
  (`{"$size": "$field"}`) in addition to the one-element array form.
- Document that a plain unique index has the same semantics as MongoDB's
  `unique + sparse`, including the explicit-`null` caveat.

## v0.28.0 - 2026-11-22

- Add opt-in SQ8/int8 ANN record encoding through
  `NONODB_VECTOR_ANN_QUANTIZE=sq8`. It cuts the 500k/384D sidecar from 741 MB to
  193 MB, but at that scale it did not reduce query latency and slightly lowered
  recall, so f32 remains the default.
- Parallelize IVF k-means training across up to 16 threads and let an explicit
  `NONODB_VECTOR_ANN_CLUSTERS` value select the target cluster count instead of
  only acting as a cap. A 500k/384D build with 4,096 clusters dropped from over
  20 minutes to 210 s; the default 707-cluster build takes 40 s.
- Add `scripts/vector_compare`, a reproducible harness comparing NonoDB exact
  and IVF vector search with Qdrant HNSW and Elasticsearch `dense_vector` HNSW
  on real MS MARCO passages and queries. On 500k passages with 384-dimensional
  MiniLM embeddings, NonoDB ANN reached recall@10 0.958 at 8.06 ms p50 versus
  2.26 ms for Qdrant and 4.47 ms for Elasticsearch at comparable recall. The
  exact NonoDB path took 346.86 ms p50.
- Read only segment format v3. Older v2 text segments are rejected instead of
  falling back to document-based phrase verification. The
  `NONODB_SEGMENT_POSITIONS` switch is removed; text segments always store
  positions. Rebuild old text indexes.
- Remove the legacy LMDB text/fuzzy search backend. Text and fuzzy definitions
  now always use immutable segments; the `SearchEngine` field, LMDB posting
  paths, `NONODB_SEGMENT_TEXT`, `NONODB_SEGMENT_FUZZY`, and the in-place
  migration startup options are gone. Search indexes now cover exactly one
  field, and MongoDB-style multi-field text keys and non-default weights are
  rejected. Rebuild or drop old search indexes before upgrading. Scheduled and
  CLI backups that include search sidecars still work.
- Fill missing exact BM25 statistics automatically at daemon startup instead of
  waiting for `NONODB_REBUILD_SEARCH_STATS=1`. The variable now supports
  `force` for a full recompute and `0` to disable the automatic fill.
- Include search sidecars in native CLI and scheduled backups by default.
  CLI backups accept `--without-search-sidecars`; scheduled jobs use
  `include_search_sidecars: false`. Age-encrypted jobs encrypt each sidecar
  file with the same recipient as the LMDB payload.
- Bump the vector ANN sidecar to format v2 with a checkpoint table per cluster.
  Queries split large clusters into checkpoint ranges and scan them across all
  cores instead of using one thread per probed cluster. The 100M benchmark drops
  from about 27 ms p50 to **5.4 ms** at the reader and **6.3 ms** through the
  daemon, with p95 **8.6 ms** / **9.3 ms** and recall@10 **1.0**. Sidecars in
  the old v1 format are incompatible and must be rebuilt once.
- Specialize vector distance loops by metric: cosine and dot no longer compute
  squared L2, and L2 no longer computes a dot product.
- Cache the memory-mapped ANN reader per sidecar and invalidate it when the file
  changes, avoiding a fresh 8 GB mapping and repeated page-table faults on every
  query.
- Skip empty IVF clusters when probing, so repetitive or very large vector
  sets return a full result set instead of occasionally returning no hits.
- Let `NONODB_VECTOR_ANN_PROBE` override the probe count at query time without
  rebuilding the ANN sidecar.
- Add `--with-search-sidecars` to `backup`/`backup-all`, and restore those files
  automatically so large segment and ANN indexes do not need a rebuild. Scheduled
  daemon backup jobs can opt in with `include_search_sidecars`; local, S3, and
  restic destinations include the derived files, while age-encrypted and
  collection-filtered jobs reject the option because sidecars are not covered
  by payload encryption.
- Make deep verification open and check segment-backed text and fuzzy indexes
  instead of reporting their LMDB postings as missing.
- Transfer search segment and vector ANN sidecars during replication snapshot
  bootstrap, with a rebuild fallback when a source file is unavailable.
- Run legacy search-index migration across all databases and allow limiting it
  with `NONODB_MIGRATE_SEARCH_COLLECTIONS`; skip migration on non-primary nodes.
- Fix segment BM25 top-k correctness around block pruning and score ties.
  Block skipping now decodes the next block instead of ending the posting list,
  stored `f32` maxima are rounded up before pruning, and equal-score results
  retain the lowest document ids.
- Store token positions in segment text files (format v3) and verify positive
  phrases from those positions, falling back to LMDB documents for legacy v2
  segments. Position data uses a per-segment term dictionary and compact term
  varint-encoded ordinals; set `NONODB_SEGMENT_POSITIONS=0` to omit positions
  and save space.
- Document the 100M vector ANN sidecar result and the 8.84M concurrent search
  benchmark.
- Add the HTTP/JSON `drop_collection` operation. It removes documents, indexes,
  history, TTL state, and search/vector sidecars through the single writer.
- Maintain exact live BM25 document count, total length, and per-term document
  frequency for segment text indexes. Updates and deletes adjust the statistics
  transactionally, so tombstoned and duplicate document versions no longer
  inflate scores. Add `NONODB_REBUILD_SEARCH_STATS=1` to fill missing statistics
  for existing segment indexes at startup; `NONODB_REBUILD_SEARCH_STATS=force`
  recomputes every index. Use `NONODB_REBUILD_SEARCH_STATS_COLLECTIONS` to limit
  a maintenance run to selected collections. A release-daemon validation on
  full MS MARCO measured p50 **4.60 ms**, p95 **29.58 ms**, p99 **60.83 ms**,
  and **116.4 qps**, within run-to-run noise of the previous approximate path.
  With 16/100 concurrent clients the same index reached **595.2/1,213.4 qps**.
- Make the immutable segment backend the default for newly created text and
  fuzzy indexes. Existing indexes keep their stored engine. Set
  `NONODB_SEGMENT_TEXT=0` or `NONODB_SEGMENT_FUZZY=0` to keep LMDB for new
  indexes.
- Plan segment fuzzy search from the rarest `3*d + 1` trigrams instead of
  merging every trigram posting list. On 808k real MS MARCO queries this moved
  p50 from 144.51 ms to **12.07 ms** and p95 from 279.97 ms to **21.76 ms**.
- Use exit status 75 for `NONODB_REPLICATION_EXIT_ON_GAP` restarts as well, so
  `Restart=on-failure` and Docker restart policies actually resync a replica
  after a replication log gap.
- Make `replication join` request a controlled restart with exit status 75 so
  snapshot bootstrap runs before the database reopens. Add
  `scripts/replication_join_test.py` covering join against an already-populated
  primary and post-bootstrap text/fuzzy/vector search.
- Add `NONODB_REPLICATION_BOOTSTRAP_TMP` for snapshot downloads and check free
  space before download/install, with an actionable error when the temp or data
  filesystem is too small.
- Rebuild missing text/fuzzy segment files and vector ANN sidecars on a replica
  after a replication snapshot is installed; add
  `scripts/replication_sidecar_test.py` covering snapshot bootstrap with
  segment-backed text/fuzzy indexes and a vector ANN sidecar.
- Add `rebuild_missing_search_sidecars()` and
  `NONODB_REBUILD_MISSING_SEARCH=1`. After a restore, the daemon can rebuild
  missing text/fuzzy segment files and vector ANN sidecars for every database
  from LMDB documents.
- Parallelize IVF sidecar construction and precompute centroid norms. The 10M
  build time fell from 236 s to 30.5 s; the 1M build fell from 38.7 s to 11.6 s.
- Add an optional memory-mapped IVF sidecar for vector indexes. On the 10M
  16D benchmark, p50 fell from 528 ms to 9.84 ms and p95 to 12.10 ms with
  recall@10 1.0 on a 20-query sample. Build with
  `NONODB_BUILD_VECTOR_ANN=1` and tune with `NONODB_VECTOR_ANN_PROBE`;
  vector writes are tracked in a small LMDB delta and merged into ANN queries,
  with exact fallback after 4,096 changed ids.
- Add an in-place migration from legacy LMDB text/fuzzy indexes to immutable
  segment indexes. Set `NONODB_MIGRATE_SEARCH_SEGMENTS=1` at daemon startup, or
  call `migrate_search_indexes_to_segment()` from the core API.
- Add `es_bench.py` and compare the 1M-passage segment backend with
  Elasticsearch 8.15. At 16/100 clients NonoDB delivered about 35%/41% more QPS
  with lower p50/p95; Elasticsearch was faster at one client.
- Add `msmarco_bench.py --clients N` for end-to-end daemon concurrency. On a
  fresh 1M-passage segment collection, 16 clients reached **1,407 QPS**; 100
  clients reached **1,326 QPS** with p50 **66.91 ms**, showing queueing rather
  than segment-search latency at high concurrency.
- Add `segment_bench --clients N` for reader concurrency measurements. On the
  8.84M segment, aggregate throughput scales from 175.6 QPS at one client to
  1,248 QPS at 16 clients, then plateaus at 64 clients without a daemon in the
  path.
- Use global BM25 statistics across all segment files and the delta: term
  document frequency, total documents, and average document length are summed
  before scoring, so scores no longer depend on which flushed segment a hit
  came from.
- Support single-field indexed filters on segment text and fuzzy indexes,
  including equality and range operators. Filter IDs are resolved through the
  existing LMDB index and applied while scoring segment hits, so filtered top-K
  results remain correct.
- Give matching phrases a score bonus on segment text indexes. Phrase
  verification and residual filtering still load only the segment candidates
  from LMDB.
- Support phrases, negative terms/phrases, and compound or non-indexed filters
  on segment text indexes. Candidate generation stays in the segment; only
  candidates are loaded from LMDB for final phrase/filter verification.
- Support compound and non-indexed filters on segment fuzzy indexes by
  verifying fetched fuzzy candidates against the filter when no exact index
  candidate set is available.
- Add full segment compaction: when tombstones exceed one quarter of the base
  segment, rebuild all live documents from LMDB into one segment and clear the
  delta and tombstones.
- Automatically compact flushed text and fuzzy segment suffixes once four
  segments exist. Compaction rebuilds the suffix from current LMDB documents,
  clamps tombstones to the compacted generation, and skips suffixes larger than
  200,000 documents.
- Memory-map immutable fuzzy value sidecars as well, and add
  `fuzzy_segment_bench` for reproducible short-string fuzzy measurements.
- Memory-map immutable segment files instead of issuing `pread` calls for
  every block header and posting block. On the full 8.84M MS MARCO segment this
  moved p50 from 7.34 ms to **1.83 ms**, p95 from 235.28 ms to **31.99 ms**, and
  single-client throughput from 24.9 to **145.6 qps**.
- Added `FuzzySegmentBuilder`/`FuzzySegmentReader`: trigram postings in the
  immutable segment format with a sidecar value store, bounded candidate merge,
  Levenshtein verification, and distance-ordered results. Wired fuzzy indexes
  into the daemon behind `NONODB_SEGMENT_FUZZY=1`, including durable delta,
  tombstone generations, flushes, create/update/delete, restart persistence,
  and exact-equality indexed filters.
- Wired the internal segment backend into the writer and daemon behind
  `NONODB_SEGMENT_TEXT=1`. New text indexes get a durable manifest, LMDB-backed
  delta, tombstone generations, synchronous 4096-document flushes, and
  create/update/delete support. The daemon search path serves simple,
  unfiltered native BM25 queries from segments.
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
