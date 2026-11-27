# Columnar collections: plan and design

Status: draft, experimental. This document describes a proposed `columnar`
collection profile. It is not a released feature and does not change the
existing `standard`, `vault`, or `timeseries` profiles.

Acceptance criteria: [Columnar collections acceptance criteria](columnar-collections-acceptance.md).

## 1. Goal

Determine whether the bucketed columnar storage used by time-series collections
can become a general-purpose collection profile without losing any observable
behavior of a standard collection.

The profile must be a **drop-in storage choice**, not a reduced document store:

- the same document model and BSON/JSON scalar semantics;
- the same CRUD, query, projection, sort, cursor, aggregation, SQL, index,
  search, graph, TTL, history, event, replication, backup, quota, metrics,
  client, CLI, GUI, and MongoDB-wire behavior;
- the same error codes and atomicity guarantees;
- no feature that exists on a standard collection may be silently unavailable
  on a columnar collection.

"Parity" means observable behavior. The physical layout and `explain` details
may differ, but response shapes and documented semantics must not.

## 2. Hypothesis

The time-series profile is small because it stores many documents in a single
LMDB value and encodes typed columns with adaptive codecs. Time is only the
bucket key and retention dimension; it is not what makes the bytes small.

The hypothesis is that a generalized bucket-columnar profile can retain the
storage win for append-heavy, schema-stable documents while implementing the
full standard-collection contract.

The first benchmark to beat is the bundled 100k-row space test:

| Form | Size | Bytes/row |
|---|---:|---:|
| raw JSON | 17.47 MB | 183.2 |
| standard collection, no indexes | 41.59 MB | 436.1 |
| time-series, compacted | 1.02 MB | 10.69 |

Those numbers do **not** include user-supplied `_id` values, the id locator map,
secondary indexes, history, or search sidecars. A general profile must account
for all of them in every storage claim.

## 3. What is reused and what changes

### Reused from the time-series bucket format

- bucket lifecycle (open rows, sealed columnar payload);
- 256-row block-column layout;
- typed columns and null bitmaps;
- delta, delta-of-delta, Gorilla XOR, frame-of-reference bit packing, byte
  shuffle, dictionary, boolean bit packing, and optional zlib;
- per-block column pruning;
- zone maps and Bloom filters where they help query pruning;
- bucket rewrite and compaction primitives.

### Not reused as-is

- mandatory `timeField` and `metaField`;
- time-window bucket keys and series hashing;
- time-series synthetic ids;
- time-series retention and rollup semantics;
- the time-series rule that `_id` uniqueness is not enforced;
- the time-series rule that document history is not recorded;
- time-series-specific aggregation and rollup fast paths.

The production implementation should extract the shared codec and bucket
encoding into a common module, so `timeseries` and `columnar` cannot drift into
two incompatible encoders. The first prototype may temporarily call the
time-series encoder, but that is not the release architecture.

## 4. Proposed profile

Working profile name: `columnar`.

Working request:

```json
{
  "op": "create_collection",
  "coll": "events",
  "profile": "columnar",
  "columnar": {
    "bucketMaxDocs": 2048,
    "bucketMaxBytes": 524288,
    "compression": "balanced",
    "partition": "id_hash",
    "openBuckets": 16
  }
}
```

Names and defaults are draft decisions. The acceptance contract requires the
final names to be documented and covered by clients, CLI, GUI, MongoDB wire,
and `create_collection` validation.

Profile rules:

- `standard`, `vault`, and `timeseries` remain unchanged.
- An existing collection cannot be converted between profiles.
- A columnar collection is created empty. Import/migration goes through normal
  inserts.
- Older binaries must reject an unknown columnar profile cleanly, without
  corrupting or partially reading the database.
- A database containing columnar collections must still back up, restore,
  branch, verify, and compact on a binary that supports the profile.

## 5. Storage architecture

### 5.1 Databases and keys

Use a dedicated LMDB sub-database per columnar collection, analogous to
`tsb:<collection>` for time-series:

```text
colb:<collection>
```

Key families:

```text
0x00 | bucket_id                    -> open bucket rows
0x01 | bucket_id | serial            -> sealed bucket payload
0x02 | canonical_id                  -> id locator
0x03 | bucket_id | row               -> tombstone bitmap / deleted row
```

The exact key bytes are an implementation detail, but the following invariants
are not:

- a bucket is self-contained and versioned;
- an id locator points at one logical document version;
- duplicate `_id` insertion is rejected atomically;
- a crash cannot expose a partially sealed bucket as committed data;
- a bucket rewrite either commits fully or leaves the previous bucket intact.

### 5.2 Bucket partitioning

Two candidate strategies must be benchmarked before Phase 1 exits:

1. **Sequential active buckets** — one or a few append buckets, best
   compression, simple scans, but updates locate the bucket through the id map.
2. **Id-hash buckets** — `bucket_id` derived from a hash of `_id`, better
   update locality and write concurrency, but more open buckets and potentially
   lower compression.

The reference implementation should support a documented default and a
configuration override. A fixed global id map is expected initially; its
overhead must be measured with realistic `_id` values because it can be tens of
bytes per document.

If the id map dominates storage or point-read cost, the fallback design is a
hash-partitioned bucket directory with bounded overflow chains. That decision
is part of Phase 1, not an undocumented optimization.

### 5.3 Document schema

The current time-series encoder flattens nested objects into dotted paths and
stores arrays as JSON leaves. That is not sufficient for full document parity.

The columnar profile must preserve:

- arbitrary nested objects, including empty objects;
- arrays, arrays of objects, nested arrays, and `null`;
- field names containing characters that are meaningful in dotted paths;
- exact numeric representation, including `$numberInt`, `$numberLong`,
  `$numberDouble`, and Decimal128;
- dates, binary, UUID, ObjectId, Timestamp, MinKey, and MaxKey;
- `_id` values of every type accepted by standard collections.

The production format should use a structural path encoding rather than an
unambiguous dotted string, or must define and test escaping. A lossy flatten
rule is a release blocker.

Schema drift is expected. Each bucket owns its field dictionary. Missing fields
use null bitmaps. A bucket that exceeds a documented column/byte limit falls
back to a raw or overflow bucket instead of failing the insert.

### 5.4 IDs and point access

Every document gets the same `_id` rules as a standard collection:

- use the supplied `_id`, preserving its type and value;
- otherwise generate the same kind of id and return it;
- reject duplicates with the same error code;
- support lookup, update, delete, `findAndModify`, and indexes by `_id`;
- keep ids stable across updates and re-encoding.

A compact id locator map is the expected reference implementation. Its key
encoding must reuse the standard canonical id encoding. The map is part of
storage accounting and compaction.

### 5.5 Writes

- Appends go to an open bucket as raw JSON rows.
- A bucket seals when it reaches `bucketMaxDocs`, `bucketMaxBytes`, or an
  explicit seal request.
- A document larger than the bucket byte limit uses a dedicated raw/overflow
  bucket.
- `insert_many`, `write_batch`, and daemon batch writes resolve an open bucket
  once per transaction, as the time-series writer already does.
- Index, history, event, TTL, graph, and replication hooks run in the same LMDB
  transaction as the document write.
- The insert return value and ordering match standard collections.

### 5.6 Reads and query planning

A read may decode:

- one open bucket row for a point lookup;
- one block from one bucket for a locator hit;
- selected columns from sealed blocks for projections and filters;
- full matching rows for stages that need the document.

The implementation must:

- support every existing filter operator, array semantics, `$expr`, regex,
  `$jsonSchema`, dates, numerics, and BSON types;
- support projections, sort, skip, limit, `find_one`, `lookup`, cursors, and
  explain;
- preserve snapshot isolation within a read transaction;
- return documents equal to the standard profile for the same committed state,
  including field order-independent JSON equality and exact scalar types.

`explain` may report `columnar` storage and bucket/block statistics, but the
existing plan fields and counters that clients rely on must remain present.

### 5.7 Updates, upserts, deletes, and findAndModify

These operations must be semantically identical to standard collections.

The expected physical path is:

- open bucket: mutate the row in place and re-encode the open payload;
- sealed bucket: apply the change through an overlay/delta or rewrite the
  affected bucket and bump its locator;
- delete: tombstone bitmap plus id-map removal; space is reclaimed by a later
  bucket rewrite/compaction;
- upsert: create through the normal insert path and return the same result
  shape;
- `findAndModify`: return the correct before/after document and update all
  indexes atomically.

If random update latency exceeds the accepted threshold, the design must add a
hot-row overlay or smaller buckets. "It is columnar, so updates are slow" is not
an acceptable parity answer.

### 5.8 TTL, history, events, and subscriptions

- Collection-default TTL, date-field TTL, per-document `_ttl`, strict read
  visibility, TTL backfill, and history purging must work exactly as on a
  standard collection.
- Expired rows are hidden at read time even before physical reclamation.
- Whole-bucket deletion is allowed only when every row in the bucket is
  expired.
- History stores document snapshots through the existing history database and
  retention settings; history storage is not claimed as columnar-compressed.
- Automatic collection events, `watch`, streams, conditional subscriptions, and
  webhooks must observe the same create/update/delete/TTL transitions as the
  standard profile.
- TTL and retention must not invalidate ids that a client can still observe.

### 5.9 Indexes, search, and graph

All index types and commands remain available:

- secondary indexes, compound indexes, unique, sparse, partial, include,
  per-field directions, geo;
- `create_index`, `create_indexes`, `drop_index`, `list_indexes`, `reindex`;
- text/BM25, fuzzy, vector, `search`, `fuzzy`, `vector_search`, `search_all`,
  hybrid search, and their sidecars/status/rebuild paths;
- graph definitions, adjacency indexes, traversal, shortest paths, Cypher,
  graph algorithms, background jobs, rebuild status, and export.

Index maintenance may materialize a document from a bucket, but no index may be
silently unsupported. Index backfills must stream buckets and remain bounded in
memory. Sidecar rebuilds after restore/branch must work.

### 5.10 Replication, backup, restore, branch, and compaction

- The primary emits the same logical operations and ids as a standard
  collection.
- A replica applies inserts, updates, deletes, TTL expiry, and index changes
  idempotently and converges byte-for-byte where the replication contract
  requires it.
- Snapshot/bootstrap and failover work with open and sealed buckets.
- Online/offline backup, verification, restore drills, import, branch/clone,
  and compaction include the columnar sub-database and id map.
- `compact_data_dir` and online compaction reclaim deleted rows from sealed
  buckets without changing visible results.
- A crash during append, seal, update, delete, TTL, index backfill, or
  compaction is recoverable; recovery never exposes partial bucket payloads.

## 6. Compatibility matrix

This is the implementation checklist. The detailed, testable version is in the
[acceptance criteria](columnar-collections-acceptance.md).

| Area | Reuse | Work required |
|---|---|---|
| `create_collection`, `list_collections`, `drop_collection` | daemon/API | profile enum, settings, list options |
| insert / insert_many / write_batch | generic hooks | bucket writer, id generation, uniqueness |
| find / find_one / lookup / count / explain / cursors | query engine | bucket scan, column pruning, explain counters |
| filters, projection, sort, skip/limit | unchanged semantics | decode and evaluate as standard |
| update / upsert / findAndModify / delete | generic hooks | id lookup, bucket rewrite/overlay |
| aggregation and SQL | pipeline/SQL engine | materialize rows, `$out`/`$merge` writer |
| secondary/compound/unique/sparse/partial/geo indexes | generic index code | streamed backfill, locator maintenance |
| text/fuzzy/vector/search_all/hybrid | search sidecars | rebuild, status, restore |
| graph/Cypher/jobs | graph engine | document materialization and hooks |
| TTL | generic TTL hooks | per-row expiry, sweeper, bitmap |
| history | history DB | snapshot hooks and retention |
| events/subscriptions/streams/webhooks | event engine | same transitions and ordering |
| replication/failover | replication layer | apply operations, snapshots |
| backup/restore/import/branch/verify/compact | store/ops | include columnar DB and id map |
| quotas/stats/metrics | accounting | encoded/open/id/index/history accounting |
| MongoDB wire and clients | protocol adapters | profile/settings parity |
| CLI/GUI | admin UI | create/list settings and stats |
| authz/audit/isolation | daemon | no new bypasses |
| docs and migration | docs | profile guide and compatibility notes |

## 7. Phases

### Phase 0 — feasibility spike

- Benchmark the existing TS encoder on generic document shapes.
- Measure the id-map overhead with realistic `_id` values.
- Prototype one read path and one update path outside the release profile.
- Exit: a written report; no production API changes.

### Phase 1 — storage core and CRUD

- New columnar module and profile, bucket writer, id map, seal, point read,
  insert/update/delete.
- Basic filters and projections.
- Differential tests against a standard collection.
- Exit: all `AC-CORE`, `AC-DOC`, `AC-WRITE`, `AC-READ`, and `AC-CRASH` criteria.

### Phase 2 — query and aggregation

- Full filter/array/regex/`$expr`/`$jsonSchema` parity.
- Sort, skip/limit, cursors, explain.
- Full aggregation and SQL parity, including `$out`/`$merge`.
- Exit: all `AC-QUERY`, `AC-AGG`, and `AC-SQL` criteria.

### Phase 3 — indexes and search

- All index types and maintenance.
- Text/fuzzy/vector sidecars, rebuilds, status.
- Exit: all `AC-IDX` and `AC-SEARCH` criteria.

### Phase 4 — lifecycle semantics

- TTL, history, events, subscriptions, streams, webhooks.
- Graph and Cypher.
- Exit: all `AC-TTL`, `AC-HISTORY`, `AC-EVENT`, and `AC-GRAPH` criteria.

### Phase 5 — operational parity

- Replication/failover, backup/restore, branch, import/verify/compact.
- Quotas, metrics, map resize, health.
- Exit: all `AC-REPL`, `AC-OPS`, and `AC-OBS` criteria.

### Phase 6 — interfaces and migration

- HTTP/socket APIs, MongoDB wire, Python/Rust/Node/Go clients, CLI, GUI,
  migration/import.
- Docs, upgrade/rollback behavior, examples.
- Exit: all `AC-API`, `AC-CLIENT`, and `AC-UPGRADE` criteria.

### Phase 7 — hardening and release

- Fuzz the decoder, crash tests, long-running soak, resource limits.
- Benchmark against standard and time-series baselines.
- Exit: all remaining criteria, no P0/P1 defects, signed-off release gate.

## 8. Benchmark plan

Use a committed, reproducible harness. Every result records:

- CPU, memory limit, filesystem, LMDB map size, build mode;
- dataset generator and seed;
- document count, average/median/p95 size, number of fields, array share;
- number of unique `_id` values and their type/length;
- index/history/search sidecar state;
- insert mode (single, 1000-row batch, transaction size);
- compacted and un-compacted sizes;
- read/write/update/delete/aggregate latencies (p50/p95);
- peak memory and seal/compaction time.

Datasets:

1. Stable analytics-like docs (nested meta, timestamps, floats, enums, booleans).
2. Normal OLTP docs (random `_id`, status fields, moderate updates).
3. Schema-drift docs (new optional fields over time).
4. Wide/nested docs and arrays.
5. Large documents near the bucket byte limit.
6. High-string-cardinality and low-string-cardinality variants.
7. Delete-heavy and retention-heavy variants.

Initial release gates (may be tightened, not loosened without review):

- storage on dataset 1 includes ids and indexes and is at most 0.50× the
  standard profile; target is at most 0.20×;
- batch insert throughput at least 0.80× standard; single insert at least
  0.50× standard;
- sealed point read at most 3× standard p50; open point read at most 1.5×;
- single-document update at most 5× standard p50; multi-update is measured
  separately;
- full scan/aggregation throughput no worse than 0.80× standard on dataset 1;
- memory is bounded and does not grow with total collection size;
- deleting and compacting 25% of rows does not change query results and
  reclaims space after compaction.

Any result below these gates requires either an accepted design change or an
explicit release blocker. Benchmarks must not compare a warm cache against a
cold cache without labeling it.

## 9. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Random updates rewrite a whole bucket | benchmark; hot-row overlay or smaller/hash buckets |
| Id map erodes storage win | measure realistic ids; hash-bucket fallback |
| Schema drift creates thousands of columns | column limit, raw/overflow bucket, schema compaction |
| Dotted-path collisions lose data | structural paths or tested escaping; round-trip tests |
| Large single document exceeds bucket limit | raw overflow bucket |
| Query engine cannot use column pruning | column-selective decode and explain coverage |
| Index/search backfill materializes too much | stream buckets, batch, memory ceilings |
| History/TTL/events diverge from standard | differential test harness |
| Backup/replication miss the id map | include in DB copy and replication snapshots |
| Old binaries read a partially supported profile | profile/version rejection and upgrade gate |
| Compression CPU hurts write latency | adaptive compression levels, seal off the hot path |
| Columnar profile becomes a second semantic engine | shared codec/decoder and differential oracle |

## 10. Rollout

- Start as an opt-in experimental profile, hidden behind documentation and
  feature gates where appropriate.
- Never make `columnar` the default by changing an existing collection.
- No automatic conversion from `standard`; conversion is export/import.
- Keep `timeseries` unchanged; extracting shared codecs must not change its
  on-disk format or query results.
- Publish measurements only after the id map, indexes, and history are
  accounted for.
- Provide a documented rollback: export to standard, or restart with a binary
  that supports the profile; an old binary must fail safely.

## 11. Open decisions

- Final profile name and settings names.
- Default bucket partition and number of open buckets.
- Id-map versus hash-directory design after measurement.
- Array representation: JSON leaf, expanded columns, or adaptive.
- Update strategy: bucket rewrite, delta overlay, or hot-row store.
- Column-count/byte limits and overflow policy.
- History representation and whether it can ever be columnar.
- Whether a columnar collection can coexist with vault encryption.
- Which performance gates become hard release gates.

These are resolved in Phase 0/1 and recorded in this document before the
acceptance criteria are marked complete.

## 12. Definition of done

The profile is done only when:

1. Every criterion in the acceptance document passes on Linux, macOS, and
   Windows CI.
2. The differential suite runs the same tests against `standard` and `columnar`
   and finds no semantic difference.
3. All existing test suites pass with the new profile enabled where applicable.
4. The full benchmark report, including ids and indexes, is committed.
5. Backup/restore, branch, replication, failover, and crash recovery drills pass.
6. Documentation, clients, CLI, GUI, and MongoDB wire support are complete.
7. No standard-collection operation returns "unsupported for this profile".
