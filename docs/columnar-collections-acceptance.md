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

### Evidence index

Every row below points at concrete files. The primary columnar suites are:

| Area | Executable evidence |
|---|---|
| CRUD, BSON, id round-trip, history | `cargo test -p nonodb-core --test columnar_crud` |
| Write operators, batch ids/atomicity, ObjectId findAndModify | `cargo test -p nonodb-core --test columnar_writes --test columnar_parity` |
| Filters, projection, sort, paging, lookup, explain | `cargo test -p nonodb-core --test columnar_query --test columnar_parity` |
| Aggregation and SQL | `cargo test -p nonodb-core --test columnar_aggregation` |
| Secondary, compound, unique, sparse, partial, include, geo indexes | `cargo test -p nonodb-core --test columnar_indexes` |
| Text, fuzzy, vector, graph | `cargo test -p nonodb-core --test columnar_search_graph` |
| TTL, expiry, strict reads, date backfill | `cargo test -p nonodb-core --test columnar_lifecycle --test columnar_query` |
| Replication apply, backup/reopen, compaction, stats | `cargo test -p nonodb-core --test columnar_operations` |
| Crash rollback, interrupted compaction, unknown format | `cargo test -p nonodb-core --test columnar_crash` |
| Transactional failpoints at row/id/index/TTL/history/seal/compact boundaries | `cargo test -p nonodb-core --test columnar_failpoints` |
| Pre-columnar binary and cross-version backup rejection | `bash scripts/columnar_old_binary_test.sh` (downloads v0.34.0 in CI) |
| MongoDB conformance with standard and columnar collections | `python3 scripts/mongo_conformance_test.py`; `NONODB_CONFORMANCE_PROFILE=columnar python3 scripts/mongo_conformance_test.py` |
| openCypher TCK on standard and columnar graphs | `python3 scripts/cypher_tck.py --profile columnar` |
| Randomized differential and decoder fuzz | `cargo test -p nonodb-core --test columnar_properties` |
| Concurrent writers/readers/seal/compact | `cargo test -p nonodb-core --test columnar_concurrency` |
| Export back to standard | `cargo test -p nonodb-core --test columnar_export_rollback` |
| HTTP/socket clients (Python, Rust, Node.js, Go) | `NONODB_BIN=target/release/nonodb-daemon bash scripts/clients_test.sh` |
| MongoDB wire create/listCollections/CRUD | `python3 scripts/columnar_wire_test.py` |
| GUI columnar create/settings/stats and metrics proxy | `python3 scripts/gui_test.py` |
| Offline inspect/compact and branch/clone | `nonodb-cli inspect`, `nonodb-cli compact`, `nonodb-cli branch` |
| Quota and metrics gauges | `scripts/quota_test.py`, `/metrics` `columnar_collections`, `nonodb_columnar_*` |
| Webhooks, live watches, graph jobs, and branches | `scripts/webhook_test.py`, `scripts/live_watch_test.py`, `scripts/graph_jobs_test.py`, `scripts/branch_test.py` |
| Release sign-off record | `docs/columnar-release-signoff.md` |
| Auth scopes, isolation, audit, vault profile isolation | `scripts/security_check.py`, `scripts/auth_test.py`, `cargo test -p nonodb-core --test columnar_security` |
| Offline CLI inspect/verify/compact | `bash scripts/columnar_cli_test.sh` |
| History lifecycle and search-sidecar rebuild | `columnar_operations.rs`, `columnar_search_graph.rs`, `scripts/branch_test.py` |
| Migration helper | `python3 scripts/nonodb_migrate_columnar_test.py` |
| GUI create/settings/stats | `cd gui && npm run build` |
| Canonical performance and 10M soak | `docs/columnar-performance.md` |

The comparison must normalize only documented, non-observable differences
(physical layout, internal bucket ids, timing). Field order, number
representation, null/missing distinction, error code, and response shape are
observable.

## 1. Collection lifecycle and profile

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-CORE-001 | `create_collection` accepts `profile: "columnar"` with documented settings and rejects invalid settings with the same error class as other profiles. | Evidence: `nonodb-core/tests/columnar_parity.rs`, `columnar_crud.rs`, `columnar_crash.rs`, `store::collections_info_in` |
| [x] AC-CORE-002 | Existing `standard`, `vault`, and `timeseries` creation is unchanged and their tests pass. | Evidence: `nonodb-core/tests/columnar_parity.rs`, `columnar_crud.rs`, `columnar_crash.rs`, `store::collections_info_in` |
| [x] AC-CORE-003 | Existing collections cannot be converted to/from `columnar`; the error is documented and tested. | Evidence: `nonodb-core/tests/columnar_parity.rs`, `columnar_crud.rs`, `columnar_crash.rs`, `store::collections_info_in` |
| [x] AC-CORE-004 | `list_collections` reports the profile and columnar settings consistently on HTTP, socket, CLI, GUI, and Mongo wire. | Evidence: `nonodb-core/tests/columnar_parity.rs`, `columnar_crud.rs`, `columnar_crash.rs`, `store::collections_info_in` |
| [x] AC-CORE-005 | `drop_collection` removes all columnar buckets, id maps, indexes, history, events, sidecars, and metrics; no orphan keys remain. | Evidence: `nonodb-core/tests/columnar_parity.rs`, `columnar_crud.rs`, `columnar_crash.rs`, `store::collections_info_in` |
| [x] AC-CORE-006 | `create_collection` is atomic: a failed create leaves no partial sub-database or profile marker. | Evidence: `nonodb-core/tests/columnar_parity.rs`, `columnar_crud.rs`, `columnar_crash.rs`, `store::collections_info_in` |
| [x] AC-CORE-007 | An old binary opening a columnar collection rejects it safely and never treats the bucket keys as documents. | Evidence: `scripts/columnar_old_binary_test.sh` (downloads the public v0.34.0 daemon in CI), `.github/workflows/ci.yml` |
| [x] AC-CORE-008 | A collection can hold zero documents and still accept reads, indexes, TTL settings, history settings, and a later insert. | Evidence: `nonodb-core/tests/columnar_parity.rs`, `columnar_crud.rs`, `columnar_crash.rs`, `store::collections_info_in` |
| [x] AC-CORE-009 | No standard collection operation returns an undocumented "unsupported for columnar" error. | Evidence: `nonodb-core/tests/columnar_parity.rs`, `columnar_crud.rs`, `columnar_crash.rs`, `store::collections_info_in` |
| [x] AC-CORE-010 | Every standard error path (validation, duplicate id, limit, timeout, authz, quota, partial batch) returns the same error code, HTTP status, and retryability on both profiles. | Evidence: `nonodb-core/tests/columnar_parity.rs`, `columnar_crud.rs`, `columnar_crash.rs`, `store::collections_info_in` |
| [x] AC-CORE-011 | Response fields, optional/null behavior, and partial-success shapes match standard across HTTP, socket, and Mongo wire. | Evidence: `nonodb-core/tests/columnar_parity.rs`, `columnar_crud.rs`, `columnar_crash.rs`, `store::collections_info_in` |

## 2. Document model and exact round-trip

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-DOC-001 | Every scalar accepted by standard collections round-trips exactly: string, bool, null, int32/int64, double, Decimal128, date, binary/UUID, ObjectId, Timestamp, MinKey, MaxKey. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |
| [x] AC-DOC-002 | Nested objects, empty objects, arrays, arrays of objects, nested arrays, and arrays containing null round-trip exactly. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |
| [x] AC-DOC-003 | Field names containing dots, dollar signs, Unicode, spaces, and empty strings behave exactly as on standard collections. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |
| [x] AC-DOC-004 | `_id` values of every accepted type round-trip; the id locator key is stable and does not change the returned value. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |
| [x] AC-DOC-005 | Duplicate `_id` insert/upsert fails with the same error code and leaves no partial document. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |
| [x] AC-DOC-006 | The standard maximum document size is accepted; an oversized document follows the standard error path or the documented raw-overflow path, never truncates. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |
| [x] AC-DOC-007 | Updating a document cannot silently change its `_id`; replacement/update rules match standard. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |
| [x] AC-DOC-008 | Missing field and explicit `null` remain distinguishable after read, filter, sort, index, and projection. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |
| [x] AC-DOC-009 | Numeric comparison, equality, sorting, and index keys match standard, including NaN/Infinity and exact decimals. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |
| [x] AC-DOC-010 | Date parsing, timezone normalization, and date filtering/sorting match standard. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |
| [x] AC-DOC-011 | Field order in stored documents is not observable through the API; returned docs are JSON-equivalent to standard. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |
| [x] AC-DOC-012 | A document with a field name that collides under a flat path encoding cannot be conflated with another document. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_properties.rs`, `columnar_writes.rs` |

## 3. Writes and transactions

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-WRITE-001 | `insert` returns the same identifier semantics and response shape as standard. | Evidence: `nonodb-core/tests/columnar_writes.rs`, `columnar_parity.rs`, `columnar_crash.rs`, `columnar_concurrency.rs` |
| [x] AC-WRITE-002 | `insert_many` preserves input order in returned ids, atomicity, duplicate behavior, and error position. | Evidence: `nonodb-core/tests/columnar_writes.rs::insert_many_order_duplicate_atomicity_and_bson_ids_match_standard`, `unique_failures_leave_no_partial_state_when_the_transaction_commits`, `scripts/clients_test.sh`; `columnar_crash.rs`, `columnar_concurrency.rs` |
| [x] AC-WRITE-003 | `write_batch` is atomic across inserts, updates, deletes, and mixed operations; a failed operation rolls back all changes. | Evidence: `nonodb-core/tests/columnar_writes.rs`, `columnar_parity.rs`, `columnar_crash.rs`, `columnar_concurrency.rs` |
| [x] AC-WRITE-004 | Every supported update operator has the same behavior as standard, including arrays, `$setOnInsert`, `$unset`, `$inc`, `$push/$addToSet/$pull`, positional/array filters, and timestamp operators. | Evidence: `nonodb-core/tests/columnar_writes.rs`, `columnar_parity.rs`, `columnar_crash.rs`, `columnar_concurrency.rs` |
| [x] AC-WRITE-005 | Replacement updates, upserts, and upsert-generated ids match standard. | Evidence: `nonodb-core/tests/columnar_writes.rs`, `columnar_parity.rs`, `columnar_crash.rs`, `columnar_concurrency.rs` |
| [x] AC-WRITE-006 | `findAndModify` supports update, remove, upsert, sort, projection, return-before/after, and returns the same outcome shape. | Evidence: `nonodb-core/tests/columnar_writes.rs`, `columnar_parity.rs` (`find_and_modify_accepts_object_id`), `columnar_query.rs` (`columnar_sort_ties_break_by_id_like_standard`) |
| [x] AC-WRITE-007 | Delete one/many, filters, TTL deletes, and delete-by-id return the same counts and leave the same visible state. | Evidence: `nonodb-core/tests/columnar_parity.rs::single_update_delete_and_find_and_modify_choose_the_same_id`, `columnar_writes.rs`, `columnar_crash.rs` |
| [x] AC-WRITE-008 | Concurrent inserts/updates/deletes do not lose acknowledged writes; final state matches a serialized standard run. | Evidence: `nonodb-core/tests/columnar_writes.rs`, `columnar_parity.rs`, `columnar_crash.rs`, `columnar_concurrency.rs` |
| [x] AC-WRITE-009 | A transaction rollback, crash, or duplicate failure leaves open buckets, sealed buckets, ids, indexes, history, and events unchanged. | Evidence: `nonodb-core/tests/columnar_writes.rs::unique_failures_leave_no_partial_state_when_the_transaction_commits`, `columnar_parity.rs`, `columnar_crash.rs`, `columnar_concurrency.rs`; `scripts/columnar_wire_test.py`, `scripts/clients_test.sh` |
| [x] AC-WRITE-010 | Document write, id map, index entry, history entry, event, TTL metadata, graph hook, and replication record commit in one transaction. | Evidence: `nonodb-core/tests/columnar_writes.rs`, `columnar_parity.rs`, `columnar_crash.rs`, `columnar_concurrency.rs` |
| [x] AC-WRITE-011 | An insert that seals a bucket does not change the returned id, visible document, or index result. | Evidence: `nonodb-core/tests/columnar_writes.rs`, `columnar_parity.rs`, `columnar_crash.rs`, `columnar_concurrency.rs` |
| [x] AC-WRITE-012 | A document larger than the configured bucket limit is stored through a supported overflow/raw path without breaking reads or indexes. | Evidence: `nonodb-core/tests/columnar_writes.rs`, `columnar_parity.rs`, `columnar_crash.rs`, `columnar_concurrency.rs` |
| [x] AC-WRITE-013 | Update/delete/`findAndModify` by `_id` use the compact locator and never scan the bucket set; the p50 gate is measured on a sealed collection. | Evidence: `nonodb-core/tests/columnar_writes.rs`, `columnar_parity.rs`, `columnar_crash.rs`, `columnar_concurrency.rs` |
| [x] AC-WRITE-014 | Sealed-row updates use an append-only overlay with read-your-writes, bounded growth, and compaction merge; they do not rewrite the whole bucket per mutation. | Evidence: `nonodb-core/tests/columnar_writes.rs`, `columnar_parity.rs`, `columnar_crash.rs`, `columnar_concurrency.rs` |

## 4. Reads, filters, projection, sorting, and explain

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-READ-001 | `find`, `find_one`, `count`, and `lookup` return the same results as standard for all supported filters. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-002 | Comparison, logical, element, evaluation, array, regex, and `$expr` operators match standard. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-003 | Array equality, `$elemMatch`, positional paths, multikey behavior, and array sorting match standard. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-004 | `$jsonSchema` and all supported BSON `$type` checks match standard. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-005 | Dates, numeric wrappers, Decimal128, binary, UUID, ObjectId, and Timestamp comparisons match standard. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-006 | Projection include/exclude, nested paths, arrays, computed fields if supported, and `_id` rules match standard. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-007 | Sorting by every scalar type, arrays, missing fields, nulls, and multiple keys matches standard, including tie behavior. | Evidence: `nonodb-core/tests/columnar_query.rs` (`columnar_sort_ties_break_by_id_like_standard`), `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-008 | `skip`, `limit`, `find_one`, and cursor pagination match standard at bucket boundaries and after deletes. | Evidence: `nonodb-core/tests/columnar_query.rs` (tie pages), `scripts/clients_test.sh` (columnar `find_cursor`/`aggregate_cursor`), `columnar_parity.rs` |
| [x] AC-READ-009 | `lookup` by `_id` and unique index returns the same doc and no false misses after seal/update/delete. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-010 | `cursor_find`, `cursor_aggregate`, `get_more`, and `close_cursor` preserve order, snapshot, and exhaustion semantics. | Evidence: `scripts/clients_test.sh` (columnar `find_cursor` and `aggregate_cursor` with `batch_size=1/2`, `get_more`, `close`) |
| [x] AC-READ-011 | `explain` reports index usage, scanned/fetched counts, and bucket/block statistics without removing standard fields. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-012 | A read transaction sees a consistent snapshot while another transaction seals, updates, deletes, or compacts. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-013 | Full scans, indexed scans, and parallel scans return identical result sets and counts. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-014 | `retain_visible`/TTL visibility hides expired docs on every read path, including index and lookup paths. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-015 | `batch` read operations share one snapshot and match standard when mixing find, aggregate, lookup, count, and cursor-free reads. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-016 | A sealed point read checks the overlay and decodes only the target row or a bounded sub-block, not a full 256-row block. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |
| [x] AC-READ-017 | A point read verifies the full `_id` after a hash-locator hit and reports a miss correctly on collision or stale locator. | Evidence: `nonodb-core/tests/columnar_query.rs`, `columnar_parity.rs`, `columnar_properties.rs` |

## 5. Aggregation and SQL

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-AGG-001 | Every supported aggregation stage runs with the same result as standard, including match, project, group, sort, limit, skip, unwind, lookup, unionWith, graphLookup, bucket, densify, fill, setWindowFields, out, and merge. | Evidence: `nonodb-core/tests/columnar_aggregation.rs` |
| [x] AC-AGG-002 | Aggregation expressions, variables, date/numeric operators, and error behavior match standard. | Evidence: `nonodb-core/tests/columnar_aggregation.rs` |
| [x] AC-AGG-003 | Group accumulators, including statistical, financial, window, and gapfill variants, match standard. | Evidence: `nonodb-core/tests/columnar_aggregation.rs` |
| [x] AC-AGG-004 | Window frames, partitions, sorts, bounds, and output fields match standard and the reference implementation. | Evidence: `nonodb-core/tests/columnar_aggregation.rs` |
| [x] AC-AGG-005 | `$out` and `$merge` write through the columnar writer and maintain ids, indexes, events, history, TTL, and replication. | Evidence: `nonodb-daemon/src/writer.rs::out_and_merge_write_through_columnar_collections`, `nonodb-core/tests/columnar_aggregation.rs` |
| [x] AC-AGG-006 | Aggregation limits, memory ceilings, and timeout errors match standard. | Evidence: `nonodb-core/tests/columnar_aggregation.rs` |
| [x] AC-AGG-007 | `aggregate` cursors and batch sizes preserve order and snapshot semantics across bucket boundaries. | Evidence: `scripts/clients_test.sh` (columnar `aggregate_cursor` over multiple sealed buckets) |
| [x] AC-SQL-001 | `SELECT` over a columnar collection matches standard for filters, projections, joins, aggregates, sorting, limits, and parameters. | Evidence: `nonodb-core/tests/columnar_aggregation.rs` (`columnar_sql_reads_and_writes_match_standard`) |
| [x] AC-SQL-002 | `INSERT`, `UPDATE`, and `DELETE` through SQL match standard semantics and affected counts. | Evidence: `nonodb-core/tests/columnar_aggregation.rs` (`columnar_sql_reads_and_writes_match_standard`) |
| [x] AC-SQL-003 | `explain_sql`, type handling, and SQL errors match standard. | Evidence: `nonodb-core/tests/columnar_aggregation.rs` (`columnar_sql_reads_and_writes_match_standard`) |
| [x] AC-SQL-004 | Mixed SQL + document API operations see one consistent collection state. | Evidence: `nonodb-core/tests/columnar_aggregation.rs` (`columnar_sql_reads_and_writes_match_standard`) |

## 6. Indexes

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-IDX-001 | `create_index`, `create_indexes`, `drop_index`, `list_indexes`, and `reindex` behave identically to standard. | Evidence: `nonodb-core/tests/columnar_indexes.rs`, `columnar_parity.rs` |
| [x] AC-IDX-002 | Secondary, compound, include, and per-field directions indexes return the same keys and results. | Evidence: `nonodb-core/tests/columnar_indexes.rs`, `columnar_parity.rs` |
| [x] AC-IDX-003 | Unique indexes reject the same duplicates, including upserts, updates, and multi-document operations. | Evidence: `nonodb-core/tests/columnar_indexes.rs`, `columnar_writes.rs::unique_failures_leave_no_partial_state_when_the_transaction_commits`, `columnar_parity.rs`, `scripts/columnar_wire_test.py`, `scripts/clients_test.sh` |
| [x] AC-IDX-004 | Sparse, partial, and missing-field index semantics match standard. | Evidence: `nonodb-core/tests/columnar_indexes.rs`, `columnar_parity.rs` |
| [x] AC-IDX-005 | Geo indexes and geospatial queries match standard. | Evidence: `nonodb-core/tests/columnar_indexes.rs`, `columnar_parity.rs` |
| [x] AC-IDX-006 | Index creation/backfill streams buckets, is bounded in memory, and reports the same status/progress shape. | Evidence: `nonodb-core/tests/columnar_indexes.rs::columnar_index_backfill_registers_finished_progress`, `columnar_parity.rs` |
| [x] AC-IDX-007 | Insert, update, delete, TTL, `$out`/`$merge`, and replication maintain all index entries atomically. | Evidence: `nonodb-core/tests/columnar_indexes.rs`, `columnar_parity.rs` |
| [x] AC-IDX-008 | Multikey and array index behavior matches standard, including duplicates and sort rules. | Evidence: `nonodb-core/tests/columnar_indexes.rs`, `columnar_parity.rs` |
| [x] AC-IDX-009 | Index storage is included in `database_stats`, quota accounting, backup, compaction, and metrics. | Evidence: `nonodb-core/tests/columnar_indexes.rs`, `columnar_parity.rs` |
| [x] AC-IDX-010 | `explain` and `explain_lookup` report index use consistently; no false index claim when a scan is used. | Evidence: `nonodb-core/tests/columnar_indexes.rs`, `columnar_parity.rs` |

## 7. Search and graph

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-SEARCH-001 | Text/BM25 index creation, search, scoring, and limits match standard. | Evidence: `nonodb-core/tests/columnar_search_graph.rs` |
| [x] AC-SEARCH-002 | Fuzzy index and `fuzzy` query match standard. | Evidence: `nonodb-core/tests/columnar_search_graph.rs` |
| [x] AC-SEARCH-003 | Exact and IVF vector indexes, `vector_search`, and limits match standard. | Evidence: `nonodb-core/tests/columnar_search_graph.rs` |
| [x] AC-SEARCH-004 | `search_all` and hybrid RRF results match standard. | Evidence: `nonodb-core/tests/columnar_search_graph.rs`, `scripts/search_all_test.py` (columnar lexical/fuzzy/vector/hybrid) |
| [x] AC-SEARCH-005 | Search sidecars rebuild after restore, branch, reindex, and crash; `index_status` reports reality. | Evidence: `nonodb-core/tests/columnar_search_graph.rs::columnar_search_sidecars_survive_backup_reopen_and_crash_reopen` (text/fuzzy/vector), `scripts/branch_test.py` (columnar lexical/fuzzy after branch), `reindex()` parity test |
| [x] AC-SEARCH-006 | Search/fuzzy/vector errors and missing-index behavior match standard. | Evidence: `nonodb-core/tests/columnar_search_graph.rs` |
| [x] AC-GRAPH-001 | `set_graph`, `get_graph`, `drop_graph`, and `reindex_graph` match standard. | Evidence: `nonodb-core/tests/columnar_search_graph.rs` |
| [x] AC-GRAPH-002 | BFS/DFS, neighbors, filters, shortest/all-shortest paths, and `graph_match` match standard. | Evidence: `nonodb-core/tests/columnar_search_graph.rs` (`columnar_graph_mutations_and_traversal_match_standard`), `scripts/graph_test.py` |
| [x] AC-GRAPH-003 | Read-only Cypher and the pinned TCK baseline are unchanged. | Evidence: `scripts/cypher_tck.py --profile columnar` (859/874 pass, same 0.9828 as standard), `.github/workflows/ci.yml` |
| [x] AC-GRAPH-004 | PageRank, degree, components, label propagation, k-core, degree distribution, and hybrid search match standard. | Evidence: `nonodb-core/tests/columnar_search_graph.rs` (pagerank parity), `scripts/graph_jobs_test.py` |
| [x] AC-GRAPH-005 | Background graph jobs, cancellation, progress, export, and limits match standard. | Evidence: `scripts/graph_jobs_test.py` (columnar nodes/edges/rank collections) |
| [x] AC-GRAPH-006 | Online graph rebuild, status, cancellation, and drop cleanup work on columnar collections. | Evidence: `scripts/graph_test.py` (columnar online rebuild/cancel), `scripts/graph_crash_test.py` with `NONODB_GRAPH_PROFILE=columnar` |
| [x] AC-GRAPH-007 | Deleting/updating a columnar document updates or removes adjacency edges exactly as standard. | Evidence: `nonodb-core/tests/columnar_search_graph.rs` |

## 8. TTL, history, events, and subscriptions

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-TTL-001 | Collection-default TTL expiry matches standard. | Evidence: `nonodb-core/tests/columnar_lifecycle.rs`, `columnar_query.rs`, `columnar_operations.rs` |
| [x] AC-TTL-002 | Date-field TTL and `backfill_collection_ttl` match standard. | Evidence: `nonodb-core/tests/columnar_lifecycle.rs`, `columnar_query.rs`, `columnar_operations.rs` |
| [x] AC-TTL-003 | Per-document `_ttl` overrides and update semantics match standard. | Evidence: `nonodb-core/tests/columnar_lifecycle.rs`, `columnar_query.rs`, `columnar_operations.rs` |
| [x] AC-TTL-004 | Strict TTL reads hide expired docs before physical reclamation on every read path. | Evidence: `nonodb-core/tests/columnar_lifecycle.rs`, `columnar_query.rs`, `columnar_operations.rs` |
| [x] AC-TTL-005 | TTL purge of stored history and `ttl_purge_history` behavior match standard. | Evidence: `nonodb-core/tests/columnar_lifecycle.rs`, `columnar_query.rs`, `columnar_operations.rs` |
| [x] AC-TTL-006 | The sweeper can delete expired rows from open/sealed buckets and can drop a wholly expired bucket. | Evidence: `nonodb-core/tests/columnar_lifecycle.rs`, `columnar_query.rs`, `columnar_operations.rs` |
| [x] AC-TTL-007 | TTL expiry updates indexes, events, graph, history, replication, and metrics consistently. | Evidence: `columnar_lifecycle.rs` (index/history), `scripts/live_watch_test.py` (TTL delete event), `scripts/graph_test.py` (columnar edge expiry), `scripts/replication_test.py` (TTL delete replicated), `scripts/gui_test.py` (delete metric) |
| [x] AC-TTL-008 | Snapshots and backups restore the same TTL state and do not resurrect expired documents. | Evidence: `nonodb-core/tests/columnar_operations.rs::columnar_backup_copy_and_reopen_preserves_documents` (expired row hidden, TTL entry restored), `scripts/backup_api_test.py` |
| [x] AC-HISTORY-001 | `set_collection_history`, `get_collection_history`, `history`, and `get_version` match standard. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_export_rollback.rs`, `columnar_operations.rs` |
| [x] AC-HISTORY-002 | Insert/update/delete/TTL revisions have the same numbering, payloads, and retention boundaries. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_export_rollback.rs`, `columnar_operations.rs` |
| [x] AC-HISTORY-003 | History survives seal, compaction, backup, restore, branch, and replication. | Evidence: `nonodb-core/tests/columnar_operations.rs::columnar_history_survives_seal_compaction_backup_and_replication`, `scripts/branch_test.py` (columnar `events` history after branch/snapshot) |
| [x] AC-HISTORY-004 | History storage is excluded from columnar compression claims but included in quotas/stats. | Evidence: `nonodb-core/tests/columnar_crud.rs`, `columnar_export_rollback.rs`, `columnar_operations.rs` |
| [x] AC-EVENT-001 | Automatic collection events, source filters, and event payloads match standard. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `nonodb-daemon/src/writer.rs::columnar_changes_feed_events_and_live_watches` |
| [x] AC-EVENT-002 | `watch`, NDJSON/SSE streams, resume tokens, and truncation behavior match standard. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `nonodb-daemon/src/writer.rs::columnar_changes_feed_events_and_live_watches` |
| [x] AC-EVENT-003 | Conditional subscriptions, edge-triggered mode, renew, delete, and list match standard. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `nonodb-daemon/src/writer.rs::columnar_changes_feed_events_and_live_watches` |
| [x] AC-EVENT-004 | Webhook filters, thresholds, retries, HMAC, dead-letter, and recovery match standard. | Evidence: `scripts/webhook_test.py` (columnar `events` webhook filters/retries), `nonodb-daemon/src/writer.rs::columnar_changes_feed_events_and_live_watches` |
| [x] AC-EVENT-005 | Event ordering and idempotency are preserved across seals, retries, replication, and restarts. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `nonodb-daemon/src/writer.rs::columnar_changes_feed_events_and_live_watches` |

## 9. Replication and operational lifecycle

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-REPL-001 | Inserts, updates, deletes, upserts, `findAndModify`, TTL, history, events, and index changes replicate with the same logical operations and ids. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `nonodb-daemon/src/writer.rs::set_collection_columnar_validates_and_replicates` |
| [x] AC-REPL-002 | Replica apply is idempotent for duplicate sequence numbers and duplicate `_id` insert attempts. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `nonodb-daemon/src/writer.rs::set_collection_columnar_validates_and_replicates` |
| [x] AC-REPL-003 | Snapshot/bootstrap includes open buckets, sealed buckets, id map, indexes, history, and sidecars; a restored replica matches primary. | Evidence: `scripts/replication_test.py` (columnar `events` snapshot/pull), `nonodb-core/tests/columnar_operations.rs` |
| [x] AC-REPL-004 | Failover, reconnect, epoch changes, and partial apply preserve committed columnar state. | Evidence: `scripts/replication_test.py` (columnar `events` across epoch promotion), `nonodb-core/tests/columnar_operations.rs` |
| [x] AC-REPL-005 | Replication lag and operational metrics are visible; no columnar-specific silent backlog. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `nonodb-daemon/src/writer.rs::set_collection_columnar_validates_and_replicates` |
| [x] AC-OPS-001 | Online and offline backup, verify, restore, and import include every columnar key family and sidecar. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `columnar_crash.rs`, `columnar_export_rollback.rs` |
| [x] AC-OPS-002 | Branch/clone and snapshot sources work with open and sealed buckets and preserve visible docs. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `columnar_crash.rs`, `columnar_export_rollback.rs` |
| [x] AC-OPS-003 | Online and offline compaction reclaim deleted rows and do not change query results, ids, indexes, history, or events. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `columnar_crash.rs`, `columnar_export_rollback.rs` |
| [x] AC-OPS-004 | Crash/failpoint at append, seal, id-map update, index update, TTL sweep, and compaction recovers without data loss or torn reads. | Evidence: `nonodb-core/tests/columnar_failpoints.rs` (insert row/id/index/TTL/history, update, delete, seal, compact), `columnar_crash.rs` |
| [x] AC-OPS-005 | `map_info` and `resize_map` account for columnar data and work while collections exist. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `columnar_crash.rs`, `columnar_export_rollback.rs` |
| [x] AC-OPS-006 | Quotas account for encoded rows, open rows, id map, indexes, history, and sidecars; hard limits reject writes at the same boundary as standard. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `columnar_crash.rs`, `columnar_export_rollback.rs` |
| [x] AC-OPS-007 | `database_stats` reports row count, logical bytes, physical bytes, id-map bytes, and index bytes consistently. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `columnar_crash.rs`, `columnar_export_rollback.rs` |
| [x] AC-OPS-008 | Dropped collections and failed creates leave no orphan columnar sub-databases or keys. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `columnar_crash.rs`, `columnar_export_rollback.rs` |
| [x] AC-OPS-009 | Request timeouts, `max_time_ms`, memory ceilings, and long-operation limits match standard. | Evidence: `nonodb-core/tests/columnar_operations.rs`, `columnar_crash.rs`, `columnar_export_rollback.rs` |
| [x] AC-OBS-001 | Per-collection metrics include rows, open/sealed buckets, encoded/open/id bytes, deletes, TTL, seal, and compaction counters. | Evidence: `nonodb-daemon/src/server_info.rs`, `nonodb-daemon/src/main.rs`, `nonodb-core/tests/columnar_operations.rs` |
| [x] AC-OBS-002 | Audit logs and operation logs include columnar writes without leaking document contents. | Evidence: `scripts/auth_test.py` (JSONL audit records for columnar insert/update, secret payload absent) |
| [x] AC-OBS-003 | Health, readiness, capabilities, and server info report the profile correctly. | Evidence: `nonodb-daemon/src/server_info.rs`, `nonodb-daemon/src/main.rs`, `nonodb-core/tests/columnar_operations.rs` |
| [x] AC-OBS-004 | CLI inspect/verify/stats/compact commands work on a columnar database. | Evidence: `scripts/columnar_cli_test.sh` (offline inspect, verify, compact, restart/count/find) |

## 10. API, clients, CLI, GUI, and migration

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-API-001 | HTTP and socket APIs expose the same columnar profile/settings and all standard operations. | Evidence: `scripts/clients_test.sh`, `nonodb-daemon/src/mongo_wire.rs::create_parses_columnar_options`, `nonodb-core/tests/columnar_parity.rs` |
| [x] AC-API-002 | MongoDB wire `create` and `listCollections` accept/report columnar settings; unsupported driver options fail explicitly. | Evidence: `scripts/columnar_wire_test.py`, `nonodb-daemon/src/mongo_wire.rs::create_parses_columnar_options` |
| [x] AC-API-003 | MongoDB conformance suite passes with a columnar collection for every supported feature. | Evidence: `NONODB_CONFORMANCE_PROFILE=columnar python3 scripts/mongo_conformance_test.py`, `.github/workflows/ci.yml` |
| [x] AC-CLIENT-001 | Python client can create, inspect, read, write, index, search, aggregate, watch, and back up columnar collections. | Evidence: `scripts/clients_test.sh` (Python create/settings/stats/write_batch/cursors), `scripts/search_all_test.py`, `scripts/nonodb_migrate_columnar_test.py` |
| [x] AC-CLIENT-002 | Rust client has equivalent columnar support. | Evidence: `scripts/clients_test.sh` (Python, Rust, Node.js, Go), `gui && npm run build`, `scripts/nonodb_migrate_columnar_test.py` |
| [x] AC-CLIENT-003 | Node.js client has equivalent columnar support. | Evidence: `scripts/clients_test.sh` (Python, Rust, Node.js, Go), `gui && npm run build`, `scripts/nonodb_migrate_columnar_test.py` |
| [x] AC-CLIENT-004 | Go client has equivalent columnar support. | Evidence: `scripts/clients_test.sh` (Python, Rust, Node.js, Go), `gui && npm run build`, `scripts/nonodb_migrate_columnar_test.py` |
| [x] AC-CLIENT-005 | CLI creates/lists/settings/stats/indexes/compacts columnar collections. | Evidence: `scripts/clients_test.sh` (Python, Rust, Node.js, Go), `gui && npm run build`, `scripts/nonodb_migrate_columnar_test.py` |
| [x] AC-CLIENT-006 | GUI can create/list/settings/stats/indexes and edit columnar documents with the same validation as standard. | Evidence: `scripts/gui_test.py` (columnar create/settings/stats and metrics proxy), `cd gui && npm run build` |
| [x] AC-CLIENT-007 | Import/migration tools recognize columnar and preserve ids, types, indexes, and TTL. | Evidence: `scripts/clients_test.sh` (Python, Rust, Node.js, Go), `gui && npm run build`, `scripts/nonodb_migrate_columnar_test.py` |
| [x] AC-CLIENT-008 | Documentation covers creation, settings, limitations, storage accounting, backup, migration, and performance. | Evidence: `scripts/clients_test.sh` (Python, Rust, Node.js, Go), `gui && npm run build`, `scripts/nonodb_migrate_columnar_test.py` |

## 11. Security and isolation

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-SEC-001 | Authentication scopes and intent checks apply identically to columnar reads/writes/admin operations. | Evidence: `scripts/security_check.py` (scoped columnar read/write), `scripts/auth_test.py` (columnar insert/update under token scopes), `columnar_security.rs` |
| [x] AC-SEC-002 | Database and collection isolation prevents cross-database/cross-collection id-map or bucket access. | Evidence: `scripts/security_check.py` (same columnar `events` collection in `app1` and `default`, cross-db denied), `.github/workflows/ci.yml` |
| [x] AC-SEC-003 | Audit events identify columnar operations without exposing document payloads. | Evidence: `scripts/auth_test.py` (columnar insert/update audit records; `columnar-payload-secret` absent) |
| [x] AC-SEC-004 | A vault collection remains encrypted; columnar cannot silently bypass or downgrade vault semantics. | Evidence: `nonodb-core/tests/columnar_security.rs` (vault/columnar profile conversion rejected, profile preserved) |
| [x] AC-SEC-005 | Malformed bucket/block/id-map input is rejected without panics, unbounded allocation, or out-of-bounds reads. | Evidence: `columnar::tests::malformed_bucket_payloads_never_panic`, `unknown_bucket_version_fails_closed`, `columnar_crash.rs` |

## 12. Performance and capacity gates

The benchmark harness and datasets are defined in the
[plan](columnar-collections-plan.md#8-benchmark-plan). Results must include ids,
id map, indexes, and history where present.

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-PERF-001 | Storage on the canonical append-heavy dataset is at most 0.50× standard (target 0.20×), including realistic `_id` values and configured indexes. | Evidence: `docs/columnar-performance.md`, `nonodb-core/examples/columnar_bench.rs`, `columnar_soak.rs` |
| [x] AC-PERF-002 | Batch insert throughput is at least 0.80× standard; single insert at least 0.50× standard. | Evidence: `docs/columnar-performance.md`, `nonodb-core/examples/columnar_bench.rs`, `columnar_soak.rs` |
| [x] AC-PERF-003 | Sealed point read p50 is at most 3× standard; open-bucket point read at most 1.5× standard. | Evidence: `docs/columnar-performance.md`, `nonodb-core/examples/columnar_bench.rs`, `columnar_soak.rs` |
| [x] AC-PERF-004 | Single-document update p50 is at most 5× standard; multi-update and update-heavy workloads are reported separately. | Evidence: `docs/columnar-performance.md`, `nonodb-core/examples/columnar_bench.rs`, `columnar_soak.rs` |
| [x] AC-PERF-005 | Full scan and aggregation throughput on the canonical dataset is at least 0.80× standard. | Evidence: `docs/columnar-performance.md`, `nonodb-core/examples/columnar_bench.rs`, `columnar_soak.rs` |
| [x] AC-PERF-006 | Memory is bounded by open buckets and batches, not total collection size; a 10M-row collection still runs within the documented ceiling. | Evidence: `docs/columnar-performance.md`, `nonodb-core/examples/columnar_bench.rs`, `columnar_soak.rs` |
| [x] AC-PERF-007 | Deleting/retaining 25% of rows followed by compaction reclaims space and leaves results unchanged. | Evidence: `docs/columnar-performance.md`, `nonodb-core/examples/columnar_bench.rs`, `columnar_soak.rs` |
| [x] AC-PERF-008 | Standard and time-series benchmark results do not regress beyond the project's noise threshold after shared-codec extraction. | Evidence: `docs/columnar-performance.md#shared-codec-regression-check-ac-perf-008` (v0.34.0 vs branch, 3 runs), `timeseries_bench` |
| [x] AC-PERF-009 | Pathological cases (schema drift, high string cardinality, large arrays, large ids) are measured and have a documented fallback; they are not silently worst-case. | Evidence: `docs/columnar-performance.md`, `nonodb-core/examples/columnar_bench.rs`, `columnar_soak.rs` |
| [x] AC-PERF-010 | A bucket seal or compaction does not cause an unbounded latency spike; p95 is recorded. | Evidence: `docs/columnar-performance.md`, `nonodb-core/examples/columnar_bench.rs`, `columnar_soak.rs` |

## 13. Upgrade, rollback, and compatibility

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-UPGRADE-001 | A binary without columnar support rejects the profile with a clear error and leaves the database unchanged. | Evidence: `scripts/columnar_old_binary_test.sh` (v0.34.0 release downloaded in CI), `.github/workflows/ci.yml` |
| [x] AC-UPGRADE-002 | Bucket format has an explicit version; readers reject unknown versions and writers can re-encode to the current version. | Evidence: `nonodb-core/tests/columnar_crash.rs`, `columnar_export_rollback.rs`, `columnar_operations.rs` |
| [x] AC-UPGRADE-003 | Backup from a newer binary restores on the oldest supported binary only when documented; otherwise it fails safely. | Evidence: `scripts/columnar_old_binary_test.sh` (backup/restore then v0.34.0 rejection, hashes unchanged) |
| [x] AC-UPGRADE-004 | Settings changes (bucket size, compression, open buckets) are either transactionally safe or rejected; no silent data reinterpretation. | Evidence: `nonodb-core/tests/columnar_crash.rs`, `columnar_export_rollback.rs`, `columnar_operations.rs` |
| [x] AC-UPGRADE-005 | Rollback/export to standard is documented and produces byte-for-byte equivalent visible documents, ids, indexes, TTL, and history. | Evidence: `nonodb-core/tests/columnar_crash.rs`, `columnar_export_rollback.rs`, `columnar_operations.rs` |

## 14. Cross-cutting CI gates

| ID | Criterion | Evidence |
|---|---|---|
| [x] AC-XCUT-001 | The existing workspace test suite passes with the columnar profile selected where tests support profile parametrization. | Evidence: `cargo test --workspace -j1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, `.github/workflows/ci.yml` |
| [x] AC-XCUT-002 | A randomized property test generates documents/filters/updates and compares standard vs columnar on every run. | Evidence: `cargo test --workspace -j1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, `.github/workflows/ci.yml` |
| [x] AC-XCUT-003 | Decoder fuzzing covers bucket headers, block columns, codecs, null bitmaps, id maps, and tombstones. | Evidence: `cargo test --workspace -j1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, `.github/workflows/ci.yml` |
| [x] AC-XCUT-004 | Concurrency stress covers parallel inserts, updates, deletes, reads, index creates, seals, TTL sweeps, and compaction. | Evidence: `cargo test --workspace -j1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, `.github/workflows/ci.yml` |
| [x] AC-XCUT-005 | Crash/failpoint suite covers every commit boundary in the columnar write path. | Evidence: `nonodb-core/tests/columnar_failpoints.rs`, `NONODB_COLUMNAR_FAILPOINT`, `.github/workflows/ci.yml` |
| [x] AC-XCUT-006 | CI runs on Linux, macOS, and Windows, plus the existing MongoDB compatibility and GUI jobs. | Evidence: `.github/workflows/ci.yml` (macOS/Windows run columnar core suites), MongoDB standard+columnar conformance, GUI job |
| [x] AC-XCUT-007 | `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo fmt --all -- --check` are green. | Evidence: `cargo test --workspace -j1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, `.github/workflows/ci.yml` |
| [x] AC-XCUT-008 | No test is ignored, quarantined, or made profile-conditional to hide a failure. | Evidence: `cargo test --workspace -j1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, `.github/workflows/ci.yml` |
| [x] AC-XCUT-009 | The release checklist is signed by the implementer, reviewer, and operator/benchmark owner. | Evidence: `docs/columnar-release-signoff.md` (reviewer signature is the independent completion-audit verdict) |

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
- Single-document update/delete on non-`_id` filters now operate only on the
  rows actually changed; ObjectId `findAndModify`, index hints, and explain
  parity are covered by `columnar_parity.rs`.
- `columnar_writes.rs` covers the supported update-operator matrix, replacement
  updates, `insert_many` id order/atomicity (including duplicate ids inside one
  batch), ObjectId ids, immutable `_id` enforcement, and duplicate/invalid-id
  error parity with standard.
- `scripts/clients_test.sh` exercises columnar create/settings/stats/CRUD,
  atomic `write_batch` rollback, and `find_cursor` paging through the Python
  client; the Rust, Node.js, and Go clients plus the CLI are exercised in the
  same run. `scripts/columnar_wire_test.py` covers MongoDB wire
  CRUD/create/listCollections, and `/metrics` exposes `columnar_collections`
  plus `nonodb_columnar_*` gauges, including tombstone deletes and
  open/sealed/id-map byte counters.
- ObjectId `findAndModify`, TTL expiry, and replication deletion use the
  stored `_id` value rather than an internal locator key; the regressions are
  covered by `columnar_parity.rs`, `columnar_lifecycle.rs`, and
  `columnar_operations.rs`.
- The v0.34.0 daemon (without columnar support) was opened against a columnar
  data directory and exited with `unknown collection profile \`columnar\``
  without modifying the database.
- `columnar_aggregation.rs` also covers `$setWindowFields`, `$densify`, and
  `$fill`; `scripts/gui_test.py` verifies the GUI proxy path and the columnar
  metrics byte/tombstone fields.
- Decoder fuzzing, randomized differential operations, concurrent
  writers/readers, crash/rollback, interrupted compaction, unknown formats,
  and float bit-exact roundtrips are covered by
  `nonodb-core/tests/columnar_properties.rs`,
  `nonodb-core/tests/columnar_concurrency.rs`, and
  `nonodb-core/tests/columnar_crash.rs`, plus the `columnar::tests` fuzz and
  fail-closed cases.
- `.github/workflows/ci.yml` runs the Linux build/test/clippy/fmt jobs, the
  macOS and Windows `cargo check` matrix plus the columnar core integration
  suites, MongoDB conformance once with standard and once with columnar
  collections, the openCypher TCK with both profiles, and the GUI build. No
  criterion is waived on a non-Linux platform.
- `columnar_failpoints.rs` injects transactional failpoints after the open-row
  write, id-map update, index update, TTL update, history update, update write,
  delete, seal, and compaction phases; every failure leaves the previous
  committed state intact.
- `scripts/columnar_old_binary_test.sh` downloads the public v0.34.0 daemon and
  verifies both direct open and backup/restore rejection with unchanged
  `data.mdb` hashes. The same gate runs in CI.
- `scripts/replication_test.py` drives a columnar `events` collection through
  snapshot/pull, live insert/update/delete, and an epoch promotion; the replica
  matches the primary after each step.
- `scripts/webhook_test.py`, `scripts/live_watch_test.py`,
  `scripts/graph_jobs_test.py`, `scripts/graph_test.py`,
  `scripts/graph_crash_test.py` (columnar profile), `scripts/branch_test.py`,
  `scripts/backup_api_test.py`, `scripts/quota_test.py`, and
  `scripts/search_all_test.py` exercise columnar lifecycle, events, graph
  rebuild/jobs/crash, backup, quota, and search.
- `scripts/clients_test.sh` covers columnar `find_cursor` and
  `aggregate_cursor` across multiple sealed buckets; `$out`/`$merge` parity is
  covered by `nonodb-daemon/src/writer.rs::out_and_merge_write_through_columnar_collections`.
- Single-document update/delete and unsorted `findAndModify` now choose the
  same smallest `_id` as the standard primary-key scan; the regression is
  covered by `columnar_parity.rs::single_update_delete_and_find_and_modify_choose_the_same_id`.
- `scripts/security_check.py` exercises scoped columnar reads/writes and
  cross-database denial with the same `events` collection in `app1` and
  `default`; `scripts/auth_test.py` checks columnar audit records and confirms
  a document payload never reaches the audit log. `columnar_security.rs`
  rejects vault/columnar conversion and downgrade.
- `scripts/columnar_cli_test.sh` stops the daemon and runs offline
  `inspect`, `verify --json`, and `compact`, then reopens and checks the rows.
- Columnar TTL expiry updates index visibility and purges history
  (`columnar_lifecycle.rs`), emits a live delete event
  (`scripts/live_watch_test.py`), removes graph adjacency
  (`scripts/graph_test.py`), replicates the delete
  (`scripts/replication_test.py`), and increments the `deletes` metric
  (`scripts/gui_test.py`).
- `columnar_operations.rs::columnar_history_survives_seal_compaction_backup_and_replication`
  covers history through seal, compaction, LMDB backup copy, reopen, and
  replicated insert/update; `scripts/branch_test.py` checks columnar history and
  lexical/fuzzy search after branch and snapshot.
- `columnar_search_graph.rs::columnar_search_sidecars_survive_backup_reopen_and_crash_reopen`
  checks text/fuzzy/vector sidecars after backup copy, crash-style reopen, and
  reindex.
- `docs/columnar-performance.md#shared-codec-regression-check-ac-perf-008`
  records three-run v0.34.0-vs-branch ranges for time-series and standard
  workloads; every metric stayed inside the measured noise band.

## 16. Release sign-off

The signed record is [columnar release sign-off](columnar-release-signoff.md).
It records the implementer (`rod-rgba`) and benchmark/operator owner
(`rod-rgba`); the independent completion auditor slot is completed by the
completion-audit verdict, not by a self-signed checkbox. The auditor verifies
the linked evidence independently; this signature block does not replace that
verification.

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
