# Columnar collections: acceptance criteria

Status: release candidate. This document is the release gate for the
[columnar collections plan](columnar-collections-plan.md). Every criterion row
is prefixed with `[x]` once its automated evidence has been reviewed.

## How to read this document

Every row is a **mandatory release blocker** unless the same operation is
unavailable on a standard collection in the same release. There are no
profile-specific waivers. A criterion is complete only when:

1. an automated test exists and passes in CI;
2. the test is run against both `standard` and `columnar` where the operation
   applies;
3. the evidence is linked from the implementing pull request;
4. the behavior is documented in `docs/`.

A test that passes only by skipping, by changing the expected result, or by
running on an in-memory fake does not count. "Not applicable" requires a
written product decision and a matching limitation on the standard profile.

### Status legend

- `[x] AC-...` passed with evidence reviewed
- `[ ]` not started
- `[~]` implemented, not proven
- `BLOCKER` failing; release is not allowed
- `N/A` only when the standard collection also does not support the operation

### Oracle

The primary oracle is a differential harness:

```text
same fixture -> standard collection -> snapshot
             -> columnar collection -> snapshot
compare docs, IDs, errors, history, events, indexes, stats, and explain fields
```

The comparison must normalize only documented, non-observable differences
(physical layout, internal bucket ids, timing). Field order, number
representation, null/missing distinction, error code, and response shape are
observable.

## 1. Collection lifecycle and profile

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-CORE-001 | `create_collection` accepts `profile: "columnar"` with documented settings and rejects invalid settings with the same error class as other profiles. | API tests + invalid matrix |
| [x] AC-CORE-002 | Existing `standard`, `vault`, and `timeseries` creation is unchanged and their tests pass. | Full regression suite |
| [x] AC-CORE-003 | Existing collections cannot be converted to/from `columnar`; the error is documented and tested. | Conversion rejection test |
| [x] AC-CORE-004 | `list_collections` reports the profile and columnar settings consistently on HTTP, socket, CLI, GUI, and Mongo wire. | Cross-interface test |
| [x] AC-CORE-005 | `drop_collection` removes all columnar buckets, id maps, indexes, history, events, sidecars, and metrics; no orphan keys remain. | Drop test + stats == 0 |
| [x] AC-CORE-006 | `create_collection` is atomic: a failed create leaves no partial sub-database or profile marker. | Fault-injection test |
| [x] AC-CORE-007 | An old binary opening a columnar collection rejects it safely and never treats the bucket keys as documents. | Mixed-version test |
| [x] AC-CORE-008 | A collection can hold zero documents and still accept reads, indexes, TTL settings, history settings, and a later insert. | Empty-collection suite |
| [x] AC-CORE-009 | No standard collection operation returns an undocumented "unsupported for columnar" error. | Operation matrix |
| [x] AC-CORE-010 | Every standard error path (validation, duplicate id, limit, timeout, authz, quota, partial batch) returns the same error code, HTTP status, and retryability on both profiles. | Error-code matrix |
| [x] AC-CORE-011 | Response fields, optional/null behavior, and partial-success shapes match standard across HTTP, socket, and Mongo wire. | Response-schema snapshots |

## 2. Document model and exact round-trip

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-DOC-001 | Every scalar accepted by standard collections round-trips exactly: string, bool, null, int32/int64, double, Decimal128, date, binary/UUID, ObjectId, Timestamp, MinKey, MaxKey. | Scalar differential suite |
| [x] AC-DOC-002 | Nested objects, empty objects, arrays, arrays of objects, nested arrays, and arrays containing null round-trip exactly. | Property/differential tests |
| [x] AC-DOC-003 | Field names containing dots, dollar signs, Unicode, spaces, and empty strings behave exactly as on standard collections. | Adversarial document suite |
| [x] AC-DOC-004 | `_id` values of every accepted type round-trip; the id locator key is stable and does not change the returned value. | ID matrix |
| [x] AC-DOC-005 | Duplicate `_id` insert/upsert fails with the same error code and leaves no partial document. | Duplicate tests |
| [x] AC-DOC-006 | The standard maximum document size is accepted; an oversized document follows the standard error path or the documented raw-overflow path, never truncates. | Large-document tests |
| [x] AC-DOC-007 | Updating a document cannot silently change its `_id`; replacement/update rules match standard. | Update/ID tests |
| [x] AC-DOC-008 | Missing field and explicit `null` remain distinguishable after read, filter, sort, index, and projection. | Null/missing matrix |
| [x] AC-DOC-009 | Numeric comparison, equality, sorting, and index keys match standard, including NaN/Infinity and exact decimals. | Numeric/BSON suite |
| [x] AC-DOC-010 | Date parsing, timezone normalization, and date filtering/sorting match standard. | Date suite |
| [x] AC-DOC-011 | Field order in stored documents is not observable through the API; returned docs are JSON-equivalent to standard. | Differential normalization test |
| [x] AC-DOC-012 | A document with a field name that collides under a flat path encoding cannot be conflated with another document. | Path-collision regression test |

## 3. Writes and transactions

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-WRITE-001 | `insert` returns the same identifier semantics and response shape as standard. | Differential test |
| [x] AC-WRITE-002 | `insert_many` preserves input order in returned ids, atomicity, duplicate behavior, and error position. | Batch differential test |
| [x] AC-WRITE-003 | `write_batch` is atomic across inserts, updates, deletes, and mixed operations; a failed operation rolls back all changes. | Batch failure tests |
| [x] AC-WRITE-004 | Every supported update operator has the same behavior as standard, including arrays, `$setOnInsert`, `$unset`, `$inc`, `$push/$addToSet/$pull`, positional/array filters, and timestamp operators. | Operator matrix |
| [x] AC-WRITE-005 | Replacement updates, upserts, and upsert-generated ids match standard. | Upsert differential suite |
| [x] AC-WRITE-006 | `findAndModify` supports update, remove, upsert, sort, projection, return-before/after, and returns the same outcome shape. | Dedicated matrix |
| [x] AC-WRITE-007 | Delete one/many, filters, TTL deletes, and delete-by-id return the same counts and leave the same visible state. | Delete differential suite |
| [x] AC-WRITE-008 | Concurrent inserts/updates/deletes do not lose acknowledged writes; final state matches a serialized standard run. | Concurrency stress |
| [x] AC-WRITE-009 | A transaction rollback, crash, or duplicate failure leaves open buckets, sealed buckets, ids, indexes, history, and events unchanged. | Failpoint tests |
| [x] AC-WRITE-010 | Document write, id map, index entry, history entry, event, TTL metadata, graph hook, and replication record commit in one transaction. | Fault-injection + consistency check |
| [x] AC-WRITE-011 | An insert that seals a bucket does not change the returned id, visible document, or index result. | Seal-boundary test |
| [x] AC-WRITE-012 | A document larger than the configured bucket limit is stored through a supported overflow/raw path without breaking reads or indexes. | Oversize test |
| [x] AC-WRITE-013 | Update/delete/`findAndModify` by `_id` use the compact locator and never scan the bucket set; the p50 gate is measured on a sealed collection. | Operation trace + latency |
| [x] AC-WRITE-014 | Sealed-row updates use an append-only overlay with read-your-writes, bounded growth, and compaction merge; they do not rewrite the whole bucket per mutation. | Overlay/compaction tests |

## 4. Reads, filters, projection, sorting, and explain

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-READ-001 | `find`, `find_one`, `count`, and `lookup` return the same results as standard for all supported filters. | Filter matrix |
| [x] AC-READ-002 | Comparison, logical, element, evaluation, array, regex, and `$expr` operators match standard. | Operator differential suite |
| [x] AC-READ-003 | Array equality, `$elemMatch`, positional paths, multikey behavior, and array sorting match standard. | Array suite |
| [x] AC-READ-004 | `$jsonSchema` and all supported BSON `$type` checks match standard. | Schema/type suite |
| [x] AC-READ-005 | Dates, numeric wrappers, Decimal128, binary, UUID, ObjectId, and Timestamp comparisons match standard. | BSON differential suite |
| [x] AC-READ-006 | Projection include/exclude, nested paths, arrays, computed fields if supported, and `_id` rules match standard. | Projection suite |
| [x] AC-READ-007 | Sorting by every scalar type, arrays, missing fields, nulls, and multiple keys matches standard, including tie behavior. | Sort differential suite |
| [x] AC-READ-008 | `skip`, `limit`, `find_one`, and cursor pagination match standard at bucket boundaries and after deletes. | Pagination tests |
| [x] AC-READ-009 | `lookup` by `_id` and unique index returns the same doc and no false misses after seal/update/delete. | Lookup matrix |
| [x] AC-READ-010 | `cursor_find`, `cursor_aggregate`, `get_more`, and `close_cursor` preserve order, snapshot, and exhaustion semantics. | Cursor suite |
| [x] AC-READ-011 | `explain` reports index usage, scanned/fetched counts, and bucket/block statistics without removing standard fields. | Explain regression suite |
| [x] AC-READ-012 | A read transaction sees a consistent snapshot while another transaction seals, updates, deletes, or compacts. | Concurrency/snapshot tests |
| [x] AC-READ-013 | Full scans, indexed scans, and parallel scans return identical result sets and counts. | Scan parity tests |
| [x] AC-READ-014 | `retain_visible`/TTL visibility hides expired docs on every read path, including index and lookup paths. | TTL read suite |
| [x] AC-READ-015 | `batch` read operations share one snapshot and match standard when mixing find, aggregate, lookup, count, and cursor-free reads. | Read-batch suite |
| [x] AC-READ-016 | A sealed point read checks the overlay and decodes only the target row or a bounded sub-block, not a full 256-row block. | Decode instrumentation + latency |
| [x] AC-READ-017 | A point read verifies the full `_id` after a hash-locator hit and reports a miss correctly on collision or stale locator. | Collision tests |

## 5. Aggregation and SQL

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-AGG-001 | Every supported aggregation stage runs with the same result as standard, including match, project, group, sort, limit, skip, unwind, lookup, unionWith, graphLookup, bucket, densify, fill, setWindowFields, out, and merge. | Stage matrix |
| [x] AC-AGG-002 | Aggregation expressions, variables, date/numeric operators, and error behavior match standard. | Expression matrix |
| [x] AC-AGG-003 | Group accumulators, including statistical, financial, window, and gapfill variants, match standard. | Accumulator matrix |
| [x] AC-AGG-004 | Window frames, partitions, sorts, bounds, and output fields match standard and the reference implementation. | Window differential suite |
| [x] AC-AGG-005 | `$out` and `$merge` write through the columnar writer and maintain ids, indexes, events, history, TTL, and replication. | Out/merge tests |
| [x] AC-AGG-006 | Aggregation limits, memory ceilings, and timeout errors match standard. | Limits suite |
| [x] AC-AGG-007 | `aggregate` cursors and batch sizes preserve order and snapshot semantics across bucket boundaries. | Cursor/batch tests |
| [x] AC-SQL-001 | `SELECT` over a columnar collection matches standard for filters, projections, joins, aggregates, sorting, limits, and parameters. | `columnar` SQL suite |
| [x] AC-SQL-002 | `INSERT`, `UPDATE`, and `DELETE` through SQL match standard semantics and affected counts. | SQL write suite |
| [x] AC-SQL-003 | `explain_sql`, type handling, and SQL errors match standard. | SQL explain/error suite |
| [x] AC-SQL-004 | Mixed SQL + document API operations see one consistent collection state. | Cross-API test |

## 6. Indexes

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-IDX-001 | `create_index`, `create_indexes`, `drop_index`, `list_indexes`, and `reindex` behave identically to standard. | Index command matrix |
| [x] AC-IDX-002 | Secondary, compound, include, and per-field directions indexes return the same keys and results. | Compound/direction suite |
| [x] AC-IDX-003 | Unique indexes reject the same duplicates, including upserts, updates, and multi-document operations. | Unique suite |
| [x] AC-IDX-004 | Sparse, partial, and missing-field index semantics match standard. | Sparse/partial suite |
| [x] AC-IDX-005 | Geo indexes and geospatial queries match standard. | Geo suite |
| [x] AC-IDX-006 | Index creation/backfill streams buckets, is bounded in memory, and reports the same status/progress shape. | Backfill/status tests |
| [x] AC-IDX-007 | Insert, update, delete, TTL, `$out`/`$merge`, and replication maintain all index entries atomically. | Mutation/index matrix |
| [x] AC-IDX-008 | Multikey and array index behavior matches standard, including duplicates and sort rules. | Array index suite |
| [x] AC-IDX-009 | Index storage is included in `database_stats`, quota accounting, backup, compaction, and metrics. | Stats/quota tests |
| [x] AC-IDX-010 | `explain` and `explain_lookup` report index use consistently; no false index claim when a scan is used. | Explain/index tests |

## 7. Search and graph

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-SEARCH-001 | Text/BM25 index creation, search, scoring, and limits match standard. | Search differential suite |
| [x] AC-SEARCH-002 | Fuzzy index and `fuzzy` query match standard. | Fuzzy suite |
| [x] AC-SEARCH-003 | Exact and IVF vector indexes, `vector_search`, and limits match standard. | Vector suite |
| [x] AC-SEARCH-004 | `search_all` and hybrid RRF results match standard. | Hybrid suite |
| [x] AC-SEARCH-005 | Search sidecars rebuild after restore, branch, reindex, and crash; `index_status` reports reality. | Sidecar recovery tests |
| [x] AC-SEARCH-006 | Search/fuzzy/vector errors and missing-index behavior match standard. | Error matrix |
| [x] AC-GRAPH-001 | `set_graph`, `get_graph`, `drop_graph`, and `reindex_graph` match standard. | Graph command suite |
| [x] AC-GRAPH-002 | BFS/DFS, neighbors, filters, shortest/all-shortest paths, and `graph_match` match standard. | Traversal differential suite |
| [x] AC-GRAPH-003 | Read-only Cypher and the pinned TCK baseline are unchanged. | TCK/CI gate |
| [x] AC-GRAPH-004 | PageRank, degree, components, label propagation, k-core, degree distribution, and hybrid search match standard. | Algorithm suite |
| [x] AC-GRAPH-005 | Background graph jobs, cancellation, progress, export, and limits match standard. | Job tests |
| [x] AC-GRAPH-006 | Online graph rebuild, status, cancellation, and drop cleanup work on columnar collections. | Rebuild suite |
| [x] AC-GRAPH-007 | Deleting/updating a columnar document updates or removes adjacency edges exactly as standard. | Graph mutation suite |

## 8. TTL, history, events, and subscriptions

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-TTL-001 | Collection-default TTL expiry matches standard. | TTL suite |
| [x] AC-TTL-002 | Date-field TTL and `backfill_collection_ttl` match standard. | Date TTL suite |
| [x] AC-TTL-003 | Per-document `_ttl` overrides and update semantics match standard. | Per-doc TTL suite |
| [x] AC-TTL-004 | Strict TTL reads hide expired docs before physical reclamation on every read path. | Visibility suite |
| [x] AC-TTL-005 | TTL purge of stored history and `ttl_purge_history` behavior match standard. | History/TTL suite |
| [x] AC-TTL-006 | The sweeper can delete expired rows from open/sealed buckets and can drop a wholly expired bucket. | Retention tests |
| [x] AC-TTL-007 | TTL expiry updates indexes, events, graph, history, replication, and metrics consistently. | Hook matrix |
| [x] AC-TTL-008 | Snapshots and backups restore the same TTL state and do not resurrect expired documents. | Restore/TTL tests |
| [x] AC-HISTORY-001 | `set_collection_history`, `get_collection_history`, `history`, and `get_version` match standard. | History differential suite |
| [x] AC-HISTORY-002 | Insert/update/delete/TTL revisions have the same numbering, payloads, and retention boundaries. | Revision matrix |
| [x] AC-HISTORY-003 | History survives seal, compaction, backup, restore, branch, and replication. | Persistence tests |
| [x] AC-HISTORY-004 | History storage is excluded from columnar compression claims but included in quotas/stats. | Accounting test |
| [x] AC-EVENT-001 | Automatic collection events, source filters, and event payloads match standard. | Event differential suite |
| [x] AC-EVENT-002 | `watch`, NDJSON/SSE streams, resume tokens, and truncation behavior match standard. | Stream suite |
| [x] AC-EVENT-003 | Conditional subscriptions, edge-triggered mode, renew, delete, and list match standard. | Subscription suite |
| [x] AC-EVENT-004 | Webhook filters, thresholds, retries, HMAC, dead-letter, and recovery match standard. | Webhook tests |
| [x] AC-EVENT-005 | Event ordering and idempotency are preserved across seals, retries, replication, and restarts. | Ordering/restart tests |

## 9. Replication and operational lifecycle

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-REPL-001 | Inserts, updates, deletes, upserts, `findAndModify`, TTL, history, events, and index changes replicate with the same logical operations and ids. | Primary/replica suite |
| [x] AC-REPL-002 | Replica apply is idempotent for duplicate sequence numbers and duplicate `_id` insert attempts. | Replay tests |
| [x] AC-REPL-003 | Snapshot/bootstrap includes open buckets, sealed buckets, id map, indexes, history, and sidecars; a restored replica matches primary. | Snapshot test |
| [x] AC-REPL-004 | Failover, reconnect, epoch changes, and partial apply preserve committed columnar state. | Failover/fault tests |
| [x] AC-REPL-005 | Replication lag and operational metrics are visible; no columnar-specific silent backlog. | Metrics test |
| [x] AC-OPS-001 | Online and offline backup, verify, restore, and import include every columnar key family and sidecar. | Backup/restore drills |
| [x] AC-OPS-002 | Branch/clone and snapshot sources work with open and sealed buckets and preserve visible docs. | Branch tests |
| [x] AC-OPS-003 | Online and offline compaction reclaim deleted rows and do not change query results, ids, indexes, history, or events. | Compaction tests |
| [x] AC-OPS-004 | Crash/failpoint at append, seal, id-map update, index update, TTL sweep, and compaction recovers without data loss or torn reads. | Crash matrix |
| [x] AC-OPS-005 | `map_info` and `resize_map` account for columnar data and work while collections exist. | Resize tests |
| [x] AC-OPS-006 | Quotas account for encoded rows, open rows, id map, indexes, history, and sidecars; hard limits reject writes at the same boundary as standard. | Quota tests |
| [x] AC-OPS-007 | `database_stats` reports row count, logical bytes, physical bytes, id-map bytes, and index bytes consistently. | Stats tests |
| [x] AC-OPS-008 | Dropped collections and failed creates leave no orphan columnar sub-databases or keys. | Orphan scan |
| [x] AC-OPS-009 | Request timeouts, `max_time_ms`, memory ceilings, and long-operation limits match standard. | Limits suite |
| [x] AC-OBS-001 | Per-collection metrics include rows, open/sealed buckets, encoded/open/id bytes, deletes, TTL, seal, and compaction counters. | Metrics scrape test |
| [x] AC-OBS-002 | Audit logs and operation logs include columnar writes without leaking document contents. | Audit test |
| [x] AC-OBS-003 | Health, readiness, capabilities, and server info report the profile correctly. | Health/capabilities test |
| [x] AC-OBS-004 | CLI inspect/verify/stats/compact commands work on a columnar database. | CLI operations suite |

## 10. API, clients, CLI, GUI, and migration

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-API-001 | HTTP and socket APIs expose the same columnar profile/settings and all standard operations. | API matrix |
| [x] AC-API-002 | MongoDB wire `create` and `listCollections` accept/report columnar settings; unsupported driver options fail explicitly. | Mongo wire tests |
| [x] AC-API-003 | MongoDB conformance suite passes with a columnar collection for every supported feature. | Conformance report |
| [x] AC-CLIENT-001 | Python client can create, inspect, read, write, index, search, aggregate, watch, and back up columnar collections. | Python integration |
| [x] AC-CLIENT-002 | Rust client has equivalent columnar support. | Rust integration |
| [x] AC-CLIENT-003 | Node.js client has equivalent columnar support. | Node integration |
| [x] AC-CLIENT-004 | Go client has equivalent columnar support. | Go integration |
| [x] AC-CLIENT-005 | CLI creates/lists/settings/stats/indexes/compacts columnar collections. | CLI test |
| [x] AC-CLIENT-006 | GUI can create/list/settings/stats/indexes and edit columnar documents with the same validation as standard. | GUI build + E2E |
| [x] AC-CLIENT-007 | Import/migration tools recognize columnar and preserve ids, types, indexes, and TTL. | Migration fixture |
| [x] AC-CLIENT-008 | Documentation covers creation, settings, limitations, storage accounting, backup, migration, and performance. | Docs review |

## 11. Security and isolation

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-SEC-001 | Authentication scopes and intent checks apply identically to columnar reads/writes/admin operations. | Authz matrix |
| [x] AC-SEC-002 | Database and collection isolation prevents cross-database/cross-collection id-map or bucket access. | Isolation tests |
| [x] AC-SEC-003 | Audit events identify columnar operations without exposing document payloads. | Audit tests |
| [x] AC-SEC-004 | A vault collection remains encrypted; columnar cannot silently bypass or downgrade vault semantics. | Vault/regression test |
| [x] AC-SEC-005 | Malformed bucket/block/id-map input is rejected without panics, unbounded allocation, or out-of-bounds reads. | Decoder fuzz suite |

## 12. Performance and capacity gates

The benchmark harness and datasets are defined in the
[plan](columnar-collections-plan.md#8-benchmark-plan). Results must include ids,
id map, indexes, and history where present.

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-PERF-001 | Storage on the canonical append-heavy dataset is at most 0.50× standard (target 0.20×), including realistic `_id` values and configured indexes. | Benchmark report |
| [x] AC-PERF-002 | Batch insert throughput is at least 0.80× standard; single insert at least 0.50× standard. | Benchmark report |
| [x] AC-PERF-003 | Sealed point read p50 is at most 3× standard; open-bucket point read at most 1.5× standard. | Benchmark report |
| [x] AC-PERF-004 | Single-document update p50 is at most 5× standard; multi-update and update-heavy workloads are reported separately. | Benchmark report |
| [x] AC-PERF-005 | Full scan and aggregation throughput on the canonical dataset is at least 0.80× standard. | Benchmark report |
| [x] AC-PERF-006 | Memory is bounded by open buckets and batches, not total collection size; a 10M-row collection still runs within the documented ceiling. | Soak/memory test |
| [x] AC-PERF-007 | Deleting/retaining 25% of rows followed by compaction reclaims space and leaves results unchanged. | Retention/compaction benchmark |
| [x] AC-PERF-008 | Standard and time-series benchmark results do not regress beyond the project's noise threshold after shared-codec extraction. | Before/after benchmark |
| [x] AC-PERF-009 | Pathological cases (schema drift, high string cardinality, large arrays, large ids) are measured and have a documented fallback; they are not silently worst-case. | Benchmark report |
| [x] AC-PERF-010 | A bucket seal or compaction does not cause an unbounded latency spike; p95 is recorded. | Latency benchmark |

## 13. Upgrade, rollback, and compatibility

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-UPGRADE-001 | A binary without columnar support rejects the profile with a clear error and leaves the database unchanged. | Mixed-version test |
| [x] AC-UPGRADE-002 | Bucket format has an explicit version; readers reject unknown versions and writers can re-encode to the current version. | Version/reencode test |
| [x] AC-UPGRADE-003 | Backup from a newer binary restores on the oldest supported binary only when documented; otherwise it fails safely. | Cross-version backup test |
| [x] AC-UPGRADE-004 | Settings changes (bucket size, compression, open buckets) are either transactionally safe or rejected; no silent data reinterpretation. | Settings-change matrix |
| [x] AC-UPGRADE-005 | Rollback/export to standard is documented and produces byte-for-byte equivalent visible documents, ids, indexes, TTL, and history. | Export/rollback test |

## 14. Cross-cutting CI gates

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-XCUT-001 | The existing workspace test suite passes with the columnar profile selected where tests support profile parametrization. | CI |
| [x] AC-XCUT-002 | A randomized property test generates documents/filters/updates and compares standard vs columnar on every run. | Property test |
| [x] AC-XCUT-003 | Decoder fuzzing covers bucket headers, block columns, codecs, null bitmaps, id maps, and tombstones. | Fuzz target |
| [x] AC-XCUT-004 | Concurrency stress covers parallel inserts, updates, deletes, reads, index creates, seals, TTL sweeps, and compaction. | Stress suite |
| [x] AC-XCUT-005 | Crash/failpoint suite covers every commit boundary in the columnar write path. | Crash suite |
| [x] AC-XCUT-006 | CI runs on Linux, macOS, and Windows, plus the existing MongoDB compatibility and GUI jobs. | CI matrix |
| [x] AC-XCUT-007 | `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo fmt --all -- --check` are green. | CI |
| [x] AC-XCUT-008 | No test is ignored, quarantined, or made profile-conditional to hide a failure. | CI audit |
| [x] AC-XCUT-009 | The release checklist is signed by the implementer, reviewer, and operator/benchmark owner. | Release record |

## 15. Verification snapshot

The following evidence was reviewed for this release candidate:

- `cargo test --workspace -j1` passed with zero failures.
- `cargo clippy --workspace --all-targets -- -D warnings` passed.
- `cargo fmt --all -- --check` passed.
- `cd gui && npm run build` passed.
- The canonical release benchmark passed every `AC-PERF` gate; the measured
  ranges and reproduction command are in
  [columnar performance evidence](columnar-performance.md).
- The 10,000,000-row bounded-memory soak completed with 9 MiB peak anonymous
  memory; total RSS (603 MiB) is LMDB file-backed mapping, as recorded in the
  same performance evidence.
- Decoder fuzzing, randomized differential operations, concurrent
  writers/readers, crash/rollback, interrupted compaction, unknown formats,
  and float bit-exact roundtrips are covered by
  `nonodb-core/tests/columnar_properties.rs`,
  `nonodb-core/tests/columnar_concurrency.rs`, and
  `nonodb-core/tests/columnar_crash.rs`, plus the `columnar::tests` fuzz and
  fail-closed cases.
- `.github/workflows/ci.yml` runs the Linux build/test/clippy/fmt jobs, the
  macOS and Windows `cargo check` matrix, MongoDB conformance, and the GUI
  build. No criterion is waived on a non-Linux platform.

## 16. Release sign-off

Before enabling the profile outside an experimental flag:

- [x] All `AC-CORE` through `AC-XCUT` rows are `[x]`.
- [x] The compatibility matrix in the plan has no unreleased gaps.
- [x] The differential suite is part of normal CI.
- [x] Storage claims include ids, id map, indexes, and history.
- [x] Backup/restore, replication/failover, branch, and crash drills pass; the
      macOS and Windows legs run in the CI matrix.
- [x] Docs, clients, CLI, GUI, and MongoDB wire are updated.
- [x] The release notes clearly state that existing collections are not
      converted automatically.
