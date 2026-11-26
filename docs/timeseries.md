# Time-series collections

NonoDB stores time-series data in **buckets of columns**, not in one document per
sample. A bucket groups one series (the `metaField` value) and one time window,
keeps every field as a typed column, and encodes each column with the cheapest
of several codecs. The result is a document API (inserts and reads look like
ordinary JSON documents) with a storage footprint closer to a purpose-built
columnar TSDB.

## Why it compresses better

| Layer | MongoDB time-series | TimescaleDB | NonoDB time-series |
|---|---|---|---|
| Grouping | bucket document per series/window | chunk per time range | bucket per series/window |
| Physical layout | row-oriented bucket document | columnar per chunk | columnar per bucket |
| Time column | stored per measurement | delta-of-delta | delta-of-delta + varint |
| Integer counters | stored per measurement | delta / simple | delta, delta-of-delta, or frame-of-reference bit packing |
| Floats | stored per measurement | Gorilla XOR | Gorilla XOR |
| Repeated strings | stored per measurement | dictionary | dictionary + bit-packed indexes |
| Booleans | stored per measurement | bit-packed | bit-packed |
| Sparse fields | nulls per measurement | null bitmap | null bitmap, column omitted when absent |
| Final stage | snappy/zstd per bucket document | optional compression | byte shuffle + zlib per column, only when it wins |
| Meta fields | stored once per bucket | tags | stored once per bucket |

The important difference from MongoDB is that a bucket is **not a JSON document
with arrays**: meta fields are stored once, every field becomes its own column,
and codecs are chosen per column from the actual data. The important difference
from a row store is that a column that never changes (a device id, a status
enum) costs a few bits per row instead of a full string.

The encoder tries every applicable codec for a column and keeps the smallest
payload. It also tries zlib on top of that payload and only accepts it when it
is at least two bytes smaller. `compression` controls the zlib level: `fast` (1),
`balanced` (3, default), `max` (6).

### Measured compression on the bundled benchmark

`nonodb-core/examples/timeseries_compression.rs` generates 10 series, one sample
per second, with three floats, one enum string, one counter, one boolean, and a
nested meta object:

```text
rows                 : 50000
series               : 10
raw JSON bytes       : 9161040
stored columnar bytes: 210374   (4.21 bytes/row, 43.6x smaller than JSON)
row-oriented zlib    : 554825   (11.10 bytes/row)
columnar vs zlib rows : 2.64x smaller
insert wall time     : ~2.0 s (25k inserts/s, release build)
```

Reproduce it:

```bash
ROWS=50000 cargo run --release -p nonodb-core --example timeseries_compression
```

The `row-oriented zlib` line is the shape a document store with whole-row (or
whole-bucket) compression gets: it compresses repetitive JSON well, but every
float and counter still costs its own text. The columnar path is 2.6x smaller on
the same data, and the gap grows with the number of low-cardinality columns and
sampling rate.

## Creating a collection

`profile` must be `timeseries`, and a `timeseries` settings object with
`timeField` is required:

```json
{
  "op": "create_collection",
  "coll": "readings",
  "profile": "timeseries",
  "timeseries": {
    "timeField": "ts",
    "metaField": "device",
    "granularity": "seconds",
    "compression": "balanced"
  }
}
```

The same body works on the HTTP API, the socket API, and the CLI/JSON
playground. Over HTTP:

```bash
curl --unix-socket /run/nonodb.sock http://localhost/v1/write \
  -H 'content-type: application/json' \
  -d '{"op":"create_collection","coll":"readings","profile":"timeseries",
       "timeseries":{"timeField":"ts","metaField":"device"}}'
```

Replicated primaries send the settings with the collection-create operation, so
replicas keep the same bucket layout.

## Settings

| Field | Default | Meaning |
|---|---|---|
| `timeField` | required | Field holding the event time. Accepts dates (`{"$date": ...}`), RFC 3339 strings, or epoch milliseconds. |
| `metaField` | none | Field that groups rows into series. Stored once per bucket and removed from the per-row columns. |
| `granularity` | `seconds` | Bucket width preset: `seconds` = 1 h, `minutes` = 1 day, `hours` = 30 days. |
| `bucketMaxDocs` | `2048` | Seals a bucket when it reaches this many rows (max 16384). |
| `bucketMaxBytes` | `524288` | Seals a bucket when its encoded payload reaches this size (max 4 MiB). |
| `compression` | `balanced` | zlib effort for the final stage: `fast`, `balanced`, `max`. |
| `expireAfterSeconds` | none | Read-time expiry relative to the event time. Expired rows are hidden immediately; sealing drops them from the sealed payload. |

Settings are validated at creation. Invalid values return a normal query error.

## Inserting

Insert documents exactly like any other collection:

```json
{
  "op": "insert",
  "coll": "readings",
  "doc": {
    "ts": {"$date": "2026-01-01T00:00:00Z"},
    "device": {"id": "sensor-7", "site": "plant-1"},
    "temperature": 21.5,
    "status": "ok"
  }
}
```

Behavior:

- the document must contain `timeField`; a missing or non-date value is rejected;
- `_id` is optional. Without it, reads return a deterministic synthetic `_id`
  derived from the series hash and row position. A supplied `_id` is stored in
  its own column and returned as-is. Time-series collections do **not** enforce
  `_id` uniqueness (matching MongoDB time-series semantics);
- rows are appended under a single LMDB key, so an insert is O(1) and does not
  re-encode the bucket;
- a bucket seals (columnar encode) when it reaches `bucketMaxDocs`, when its
  payload exceeds `bucketMaxBytes`, or when the time window advances.

## Reading

`find`, `count`, and `explain` work with the normal filters. Buckets whose time
range cannot overlap the filter are skipped without decoding, and a filter that
pins `metaField` by equality restricts the scan to that series.

```json
{
  "op": "find",
  "coll": "readings",
  "filter": {
    "device.id": "sensor-7",
    "ts": {"$gte": {"$date": "2026-01-01T00:00:00Z"},
           "$lt":  {"$date": "2026-01-01T01:00:00Z"}},
    "temperature": {"$gt": 20}
  },
  "sort": {"ts": -1},
  "projection": {"temperature": 1, "ts": 1},
  "limit": 1000
}
```

Residual filters, sorting, `skip`/`limit`, and projections are applied after
decoding.

### Aggregation and downsampling

Aggregation pipelines run over the decoded documents, so `$group`, `$bucket`,
`$sort`, `$project`, `$count`, and the rest of the stage set work on time-series
collections. A leading `$match` is reused for bucket and series pruning.
Row-local stages are fed in batches by `scan_batches`, and a `$group` stage
streams every supported accumulator (including `$first`, `$last`, `$push`, and
`$addToSet`) into one group builder, so downsampling does not materialize the
range; `$sort`/`$limit` then operate on the much smaller group result. Other
blocking stages use the materialized path:

```json
{
  "op": "aggregate",
  "coll": "readings",
  "pipeline": [
    {"$match": {"device.id": "sensor-7",
                "ts": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}}},
    {"$group": {"_id": "$status", "samples": {"$sum": 1},
                "avg_temp": {"$avg": "$temperature"}}},
    {"$sort": {"_id": 1}}
  ]
}
```

Downsampling to per-minute buckets works the same way (group by a truncated
time expression computed with `$dateToString`/`$subtract` in `$project`).

Stages that need a second collection or an index host run through the normal
aggregation host: `$lookup` (local and pipeline/`let`), `$graphLookup`,
`$unionWith`, `$search`, `$searchMeta`, and `$vectorSearch` work on time-series
inputs. `$out` and `$merge` write through the bucket writer, so indexes,
events, and replication stay consistent.

## Supported operations

| Operation | Time-series collection |
|---|---|
| `insert` / `insert_many` / `write_batch` inserts | yes |
| `find`, `find_raw`, `find_by_key`, ordered lookups, `count`, `explain` | yes |
| projections, sorts, `skip`/`limit`, `maxTimeMS`, hints | yes (residual, after decode) |
| TTL: `expireAfterSeconds` on the TS settings | yes (read-time) |
| TTL: collection/document TTL | not applied to buckets |
| `verify_data_dir` | decodes every bucket and validates keys, headers, rows, and deletion bitmaps |
| document history | not recorded (keeps the format columnar) |
| secondary indexes (`createIndex`/`dropIndex`/`reindex`) | yes, including unique |
| unique indexes | enforced on insert/update/upsert/reindex |
| text, fuzzy, geo, vector indexes | yes (`createTextIndex`, `createFuzzyIndex`, `createGeoIndex`, `createVectorIndex`) |
| `$search`, `$searchMeta`, `$vectorSearch` aggregation stages | yes |
| `delete` by filter or by `_id` | yes (bitmap; `_id` uses the locator directly) |
| `findAndModify` with `remove: true` | yes |
| `findAndModify` with an update | yes (returns the new `_id` when the row moves) |
| `update` (`$set`, `$inc`, `$mul`, `$unset`, `$rename`, pipelines, upsert) | yes |
| aggregation pipelines (`$match`, `$group`, `$bucket`, `$sort`, ...) | yes; row-local stages and `$group` stream in batches, other blocking stages fall back to memory |
| retention: delete rows older than a cutoff | `retain_timeseries` |
| SQL `SELECT`/`INSERT`/`UPDATE`/`DELETE`, `explain` | yes (same find/aggregate path) |
| `$search`, `$searchMeta`, `$vectorSearch` | yes |
| `$lookup` (local/foreign and pipeline/`let`), `$graphLookup`, `$unionWith` | yes |
| `_events`, change streams/watch, persistent and live subscriptions | yes |
| webhook rules with count/sum/avg thresholds | yes |
| drop collection | removes all buckets and series pointers |
| replication, backup, restore, verify | yes (buckets travel with the database) |

Rejections return `invalid query: `<op>` is not supported on time-series
collection `<name>``, so API clients fail loudly instead of silently returning
empty results.

### Secondary indexes

`createIndex`/`createIndexes` build secondary indexes over time-series rows;
`dropIndex`, `listIndexes`, and `reindex` work as on document collections.
Backfills decode one bucket at a time and hand bounded row batches to the
index writer; they do not build an `all_docs()` vector for the whole
collection. The planner uses an index when the filter has an equality/range
condition on the leading field and reports `"index_used": true` with
`"access": "index_scan"` in `explain`. A partial index is considered when the
query contains its predicate or a range bound at least as selective as it (for
example, a query `ts >= 150` can use an index filtered by `ts >= 100`). Index
entries are maintained by
`insert`, `delete`, `update`, upserts, and replicated writes; a row moved by an
update removes its old entry and adds a new one. Retention removes index
entries for every reaped row (whole dropped buckets and the tombstoned boundary
bucket) before returning, so `reindex` is not required to avoid stale keys. Unique indexes are enforced on insert, update, upsert, and reindex; deleted
rows free their value immediately.

### Search indexes

Time-series collections accept the same text, fuzzy, geo, and vector indexes as
document collections. Creation backfills them from decoded bucket rows and the
regular search APIs (`search`, `fuzzy_search`, `vector_search`, `$near`,
`$geoWithin`) plus the `$search`/`$searchMeta`/`$vectorSearch` aggregation
stages return the same documents as a scan. Inserts, updates (including rows
that move buckets), deletes, upserts, and replicated writes maintain the
entries; `reindex` rebuilds all of them. Text/fuzzy/geo/vector indexes have no
uniqueness semantics of their own.

### GUI and CLI

The GUI's **New collection** dialog offers `timeseries` next to `standard` and
`vault`; it requires a time field and collects meta field, granularity,
compression, and `expireAfterSeconds`, sending the same `timeseries` object as
the wire API. Opening a time-series collection and clicking **Settings** shows
the stored settings plus row/bucket/series counts, deleted rows, encoded size,
and raw JSON size; compression, bucket size, and expiry are editable (the
structural time/meta/granularity fields are read-only once rows exist).

### Reading and changing settings

`Store::timeseries_settings("name")` and the `get_collection_timeseries` wire op
return the saved settings. `set_collection_timeseries` (or
`Collection::set_timeseries_settings`) validates and persists a replacement and
replicates it as a settings operation. `timeField`, `metaField`, and
`granularity` are structural: they can only change while the collection is
empty because existing buckets were encoded with the old layout. Bucket size
limits, compression level, and `expireAfterSeconds` can change at any time.
MongoDB clients can pass the standard `timeseries` object to `create`:
`timeField`, optional `metaField`, `granularity`, and `expireAfterSeconds`.

### Change events, watches, and webhooks

Time-series writes use the same event pipeline as document collections.
Enabling events for a time-series collection writes `insert`/`update`/`delete`
entries to `_events` with the decoded document as `payload.before`/`after`;
when an update moves a row to another bucket, the emitted `_id` is the new
synthetic id. Persistent subscriptions (`CreateSubscription`) and live
subscriptions (including `count` thresholds evaluated through the normal
`count` path) fire for those events, so `watch` long-poll, NDJSON, and SSE
streams deliver them unchanged. Webhook rules can match the collection and use
`count`, `sum`, or `avg` thresholds over row fields; the daemon worker posts
deliveries with the threshold value in `webhook.threshold_value`. Event
projections, filters, and `fields` filters apply exactly as for document
collections.

### Joins and set stages

Aggregation on a time-series collection runs with the same host as document
collections, so `$lookup` (both `localField`/`foreignField` and
`pipeline`/`let` forms), `$graphLookup`, and `$unionWith` work. The foreign
collection can be a document collection or another time-series collection;
`find` on it uses its own bucket pruning and indexes. `$out`/`$merge` targets
write through the bucket writer, including indexes and events.

### SQL

`sql::execute` and the daemon's `sql` API work on time-series collections.
`SELECT` reuses the collection read path, so WHERE pruning, secondary indexes,
sorting, `LIMIT`/`OFFSET`, `GROUP BY`, and `HAVING` behave the same as the
native API; `INSERT`, `UPDATE`, and `DELETE` route through the time-series
write path and therefore keep buckets, indexes, and ids consistent. `EXPLAIN`
for a simple `SELECT` reports the bucket scan or `index_scan` plan.

### Updates and ids

A time-series `_id` encodes the row location (bucket + position), so an update
behaves differently depending on what it touches:

- Changing ordinary fields rewrites the affected sealed bucket in place; the
  row keeps its position and its `_id` stays valid. Pending rows are rewritten
  under their existing key.
- Changing `timeField` or `metaField` moves the row to another bucket: the old
  row is marked deleted, the document is re-inserted, and its synthetic `_id`
  changes. `update` still reports `matched`/`modified`; `findAndModify` returns
  the new `_id` in `new_id` and the updated document when `new: true`. The
  change is replicated as `delete(old)` + `insert(new)`.
- Removing the time field (`$unset`) or setting it to a non-date value is
  rejected.
- Upserts accept `$setOnInsert` and must end up with a valid time field.
- Secondary, text, fuzzy, geo, and vector indexes are maintained by inserts,
  updates, upserts, deletes, `findAndModify`, and replicated writes, including
  moves and index rebuilds. History is not recorded for time-series rows; TTL
  is read-time expiry (`expireAfterSeconds`) plus automatic daemon retention,
  not document history.

## Disk usage

Same 100k-row workload (10 series, 8 columns, 183 bytes of JSON per row),
`ROWS=100000 cargo run --release -p nonodb-core --example timeseries_space`:

| Form | Size | Bytes/row |
|---|---|---|
| raw JSON documents | 17.47 MB | 183.2 |
| standard collection (`data.mdb`, no indexes) | 41.35 MB | 433.6 |
| TS, pending rows before seal (`data.mdb`) | 7.53 MB | 78.9 |
| TS, sealed, before compaction (`data.mdb`) | 7.53 MB | 78.9 |
| TS, sealed, compacted copy | **0.53 MB** | **14.75** |
| TS after deleting 25k rows (bitmap), compacted | 1.00 MB | 26.7 |
| TS after retention leaves 37.5k rows, compacted | 0.53 MB | 14.75 |

Read that carefully, because it matters operationally:

- **Sealed payloads are tiny, but LMDB never shrinks `data.mdb`.** Sealing and
  retention free pages for reuse; the file keeps the high-water mark. The
  numbers above only drop after a compacting copy.
- **Compaction is the existing backup path.** `nonodb-cli backup` and daemon
  backup jobs call `copy_to_file(..., CompactionOption::Enabled)`, so a
  backup/restore cycle reclaims the space. `Store::copy_to_file` exposes the
  same operation for scripts.
- **Pending rows are raw JSON on purpose** (O(1) inserts). With
  `bucket_max_docs = 2048`, at most 2048 rows per series are uncompressed at
  any moment; seal or `seal_timeseries` shrinks them into the columnar payload.
- **Updates and `_id` deletes decode one bucket**, not the whole collection.
  An update by `_id` rewrites the affected sealed bucket (about 0.7 s for a
  1,667-row bucket in the 100k-row benchmark); changing time/meta is a move.
- Compaction is a whole-database operation. The daemon runs it online once the
  database is idle; for a manual window use `nonodb-cli compact`, or compact a
  replica, exactly like any other LMDB high-water issue.
- Deleted rows are bits in a per-bucket deletion bitmap appended to the columnar
  payload (one bit per row, so 1000 deleted rows cost 125 bytes instead of
  27-byte tombstone keys). Deleted row values are cleared on the next bucket
  write, so their columns stop occupying space even though the sequence slots
  remain (synthetic ids stay valid). Retention uses the same path for the
  boundary bucket; the whole window is dropped once it ages out. Buckets written by older
  versions keep separate `0x03` tombstone keys; reads accept both formats and
  any write/seal folds the old keys into the bitmap.

### Reclaiming space

Stop the daemon and compact in place:

```bash
nonodb-cli compact /var/lib/nonodb/default
```

The command copies `data.mdb` through LMDB's compacting copy, fsyncs it, swaps it
in, and keeps `data.mdb.before-compact` until the swap succeeds. It refuses to
run against a live environment. `nonodb-cli backup <data-dir> <out-dir>` produces
a compacted copy without touching the source, which is the safer option on a
primary.

Compacted on-disk size is **~89x smaller than a document-per-row collection**
and **~38x smaller than the raw JSON** for this workload. Deleting 25,000 rows
with the bitmap took ~1.26 s and left the sealed file at its high-water mark
until the compacting copy above.

## Deleting rows

`delete` works on time-series collections. Pending rows are removed from their
key; rows inside sealed buckets get a **deletion bitmap bit** in the bucket
payload instead of a rewrite:

```json
{"op": "delete", "coll": "readings", "filter": {"device.id": "sensor-7", "status": "error"}, "multi": true}
```

Why a bitmap: synthetic `_id`s encode the row position inside a bucket, and a
rewrite would renumber every following row. Setting a bit keeps sealed buckets
logically immutable (the column data is never renumbered), so previously
returned `_id`s stay valid, and a delete costs one bucket re-encode instead of
one LMDB key per deleted row. A full bucket that becomes empty is dropped
outright. `find`, `count`, `find_by_key`, aggregation, and `verify_data_dir` all
skip deleted rows. Deleted values are cleared when the bucket is re-encoded;
`Store::compact_timeseries_buckets(name, min_deleted)` rewrites buckets that
still contain deleted payload and returns a `BucketCompactReport` with
before/after/reclaimed bytes. `timeseries_stats()` reports deleted rows.

The bucket format is `NTB2` (bitmap appended after the columns). `NTB1` payloads
and legacy `0x03` tombstone keys are still read; the next bucket write, delete,
retention pass, or seal migrates them into the bitmap. Space from deleted rows
and sealed payloads is reclaimed when the bucket's whole window ages out
(retention drops the bucket) or by compaction — see below. `delete` with
`multi: false` removes at most one row.

## Retention and compaction

`retain_timeseries` deletes every row older than a cutoff. Buckets whose whole
window is older are dropped by key with **no column decode**; the boundary
bucket keeps its immutable payload and marks only the expired rows deleted, so
synthetic ids remain valid. Pending rows are inspected individually.

The daemon runs retention automatically for every time-series collection whose
settings have `expireAfterSeconds` > 0. The schedule is controlled by:

| Variable | Default | Meaning |
| --- | --- | --- |
| `NONODB_TS_RETENTION_INTERVAL_SECS` | `60` | How often to check collections; `0` disables automatic retention and compaction checks. |
| `NONODB_TS_COMPACT_THRESHOLD_MB` | `256` | Size of `data.mdb` above which deleted time-series pages trigger a compaction request; `0` disables the request. |
| `NONODB_TS_ONLINE_COMPACT_INTERVAL_SECS` | `30` | How often the daemon looks for compaction markers and compacts an idle database; `0` disables online compaction. |

Retention runs online through the single writer thread, so `find`, cursors, live
subscriptions, and replication keep working. LMDB does not shrink `data.mdb`
when rows are deleted, so when deletions were observed and the file is at or
above the threshold the daemon writes `.timeseries-compact-requested` and logs a
warning. The online compactor picks the marker up within
`NONODB_TS_ONLINE_COMPACT_INTERVAL_SECS`. It removes the database runtime from
the pool, refuses to swap while any request still holds it, joins the writer and
maintenance threads (closing the LMDB environment), runs the compacting copy,
restarts the runtime, and inserts it back under the same database name. Active
connections therefore finish against the old runtime instead of being
interrupted; the marker stays until the new file is fsynced and swapped in.

The same compacting copy is also used before the environment is opened on
startup, so a marker left by a crash or a disabled compactor is handled on the
next start. `nonodb-cli compact <data-dir>` remains available while the daemon is
stopped. The swap keeps `data.mdb.before-compact` until it succeeds and leaves
the request marker in place if compaction fails so
the next start retries.

```json
{
  "op": "retain_timeseries",
  "coll": "readings",
  "before": {"$date": "2026-01-01T00:00:00Z"}
}
```

Response:

```json
{"ok": true, "result": {"coll": "readings", "before_ms": 1767225600000,
 "report": {"dropped_buckets": 20, "tombstoned_buckets": 10,
            "dropped_rows": 0, "remaining_rows": 64000}}}
```

Retention is replicated as `time_series_retain`, so replicas drop the same
ranges. Run it from a scheduler (workers/cron) for a rolling window, or call it
once before an archive. Compact afterwards (backup path) if you need the file to
shrink on disk.

## Replication and backups

Time-series collections participate in the normal replication stream:

- `CollectionCreate` carries the serialized settings, so a replica creates the
  same bucket layout instead of falling back to a standard collection;
- document inserts are applied through the bucket writer, not into the (empty)
  `coll:<name>` database, so replicas are readable immediately;
- inserts are deterministic: same settings, same insert order, same timestamps
  produce the same synthetic `_id` on primary and replica (covered by a test
  that compares full document sets);
- `time_series_retain` replicas the retention cutoff;
- snapshot bootstrap, full backups, restores, and branch clones copy the whole
  environment, so `tsb:<collection>` buckets travel with the database;
- filtered backups drop collections through the normal drop path, which also
  clears the bucket database.

`verify_data_dir` decodes every sealed bucket, checks the header (window bounds,
meta), validates every row still has its time field, validates pending-row keys,
deletion bitmaps, and legacy tombstones, and reports live row counts for
time-series collections.

## Maintenance

Open buckets keep rows as raw JSON between inserts. That is intentional: an
insert must not rewrite a whole bucket. Sealing is automatic, but an operator
can force it (for example before a cold backup or a compression report):

```rust
store.seal_timeseries("readings")?;
```

`Collection::timeseries_stats()` returns buckets, sealed/open counts, series,
rows, raw bytes, sealed bytes, and pending bytes, plus `compression_ratio()`
based on total stored bytes. `Collection::storage_stats()` reports the bucket
database for the GUI and metrics.

## Format

Buckets live in a dedicated LMDB sub-database `tsb:<collection>`:

```text
0x00 | meta_hash(8) | salt(4)                       -> open bucket pointer + row count
0x01 | meta_hash(8) | salt(4) | window(8) | serial(2) -> sealed columnar bucket
0x02 | meta_hash(8) | salt(4) | window(8) | serial(2) | seq(4) -> pending row (raw JSON)
```

`meta_hash` is a stable FNV-1a 64-bit hash of canonical JSON for the series
value; `salt` disambiguates hash collisions and keeps the on-disk key
independent of the Rust hasher version.

A sealed bucket stores a versioned header (row count, window start/end, meta
value) followed by one block per column: field name, kind (`i64`, `f64`, `bool`,
`string`, `json`), codec, optional null bitmap, and a length-prefixed payload.
Every payload may additionally be zlib-deflated (codec flag `0x80`). Unknown
versions or codecs are reported as corruption instead of being guessed.

## Write and read speed

Measured with `nonodb-core/examples/timeseries_bench.rs` on 100k rows, 10
series, 8 columns (release build, single core, local SSD), compared with a
standard collection holding the same documents:

```text
write
insert (time-series)                 100000 ops   17552 ops/s  p50 37.9us  p95 64.7us  p99 74.4us
insert (standard, baseline)          100000 ops   39789 ops/s  p50 21.7us  p95 38.2us  p99 44.8us
seal open buckets                        10 perm  231 ms total, 23.1 ms/bucket
stored: 9.24 bytes/row, 19.8x smaller than raw JSON

read (time-series collection)
find_by_key (point)                     2000 ops    2505 ops/s  p50 352 us  p99 613 us
find 1 min, 1 series (60 rows)          2000 ops     197 ops/s  p50 4.81 ms  p99 8.20 ms
find 1 h, 1 series (3600 rows)           500 ops      48 ops/s  p50 20.2 ms  p99 30.5 ms
count 1 h, 1 series                      500 ops      36 ops/s  p50 27.2 ms  p99 43.7 ms
aggregate $group 1 h, 1 series           200 ops      20 ops/s  p50 48.9 ms  p99 61.5 ms
aggregate streamed $match/$project       200 ops      23 ops/s  p50 43.6 ms  p99 64.5 ms
delete by _id (bitmap)                    50 ops      66 ops/s  p50 15.1 ms  p99 17.2 ms
find match-all (99950 rows)               40 ops     2.4 ops/s  p50 411 ms   p99 565 ms
retention before +1 h                      1 pass   541 ms, 20 dropped + 10 tombstoned

index, update, and SQL
createIndex status                         1 call   757 ms
find indexed status+series (100 rows)    200 ops    25.7 ops/s  p50 30.8 ms  p99 107 ms
explain indexed query                               index_used=true, access=index_scan
update $set by _id (100 rows)            100 ops    56.2 ops/s  p50 17.6 ms  p99 19.4 ms
SQL GROUP BY status (4 groups)            20 ops     1.7 ops/s  p50 585 ms   p99 646 ms

baseline (standard collection, one document per row)
find 1 h, 1 series (no index)             40 ops     3.6 ops/s  p50 282 ms   p99 286 ms
find match-all (100000 rows)              40 ops     2.3 ops/s  p50 434 ms   p99 442 ms
```

Reading a one-hour slice is ~13x faster than the document baseline because
whole buckets outside the time range are skipped instead of parsing 100k JSON
documents. Inserts are ~2x slower than the raw document path: each insert is two
LMDB puts (row + open-bucket pointer) instead of one, and series resolution
reads the pointer.

Point lookup by `_id` decodes one 256-row column block (about 0.35 ms in the
100k-row benchmark), not the whole bucket. User-supplied ids use a small
`0x05` id-to-locator map, so they also skip the collection scan. Range scans,
counts, and downsampling remain the intended access pattern, but single-row
lookups are now practical.

An unsorted indexed query stops after `skip + limit` rows, so a broad status
query with `limit: 100` costs ~31 ms instead of decoding every candidate.
Updates and `findAndModify` by synthetic `_id` restrict the scan to the bucket
encoded in the id (~18 ms for a 1,667-row bucket), and a pure `_id` delete uses
the locator. User-supplied ids use the `0x05` id-to-locator map instead of a
collection scan.

Reproduce: `ROWS=100000 READS=500 cargo run --release -p nonodb-core --example timeseries_bench`

### Comparison with TimescaleDB

The same 100k-row workload was loaded into TimescaleDB 2.x (PostgreSQL 16,
`timescale/timescaledb:latest-pg16`) through the local TCP listener. Timescale
rows were inserted one statement at a time for the insert sample and in
1000-row batches for the rest; range/count/aggregate timings include fetching
the result. NonoDB runs in-process, so its timings exclude a network round trip.
Storage for TimescaleDB is `hypertable_size` after native compression
(`segmentby=device`, `orderby=ts DESC`).

| Operation | NonoDB standard | NonoDB time-series | TimescaleDB |
|---|---:|---:|---:|
| Insert, one row | 20 µs p50 | 39 µs p50 | 0.89 ms p50 |
| Bulk load, 1000-row txns | 94,800 rows/s | 46,000 rows/s | 98,900 rows/s |
| Find by `_id` / primary key | 5.9 µs | 355 µs | 0.62 ms |
| Find 1 h, 1 series | 358 ms | **20.2 ms** | 14.9 ms |
| Count 1 h, 1 series | 269 ms | 24.8 ms | 1.81 ms |
| Aggregate `$group` 1 h | 118 ms | 35.7 ms | 2.54 ms |
| Update by id | 25 µs | 13.6 ms | 1.46 ms |
| Delete by id | 20 µs | 14.8 ms | 1.33 ms |
| Indexed status+series, limit 100 | 0.36 ms | 33.1 ms | 1.11 ms |
| Create index on status | 209 ms | 919 ms | 222 ms |
| Storage, 100k rows | 41.35 MB | **0.53 MB** (compacted) | 1.98 MB (compressed) |

TimescaleDB wins the point/update/delete and highly selective indexed queries
because it is a general OLTP engine with native B-tree indexes. NonoDB
time-series wins on footprint and is close on range scans; its count/aggregate
path decodes buckets instead of using a columnar executor. The comparison is a
shape check, not a tuned benchmark: no PostgreSQL tuning, connection pooling, or
prepared statements were used. Reproduce the Timescale side with
`scripts/timescale_bench.py`.

## Limitations

- Deletes and rows moved by an update set bits in the bucket deletion bitmap.
  Their values are cleared on the next bucket rewrite, but LMDB still keeps the
  high-water mark, so reclaiming file space requires an online/offline compacting
  copy rather than a retention pass alone.
- Unique secondary indexes are enforced on insert, update, upsert, and reindex
  (deleted rows immediately free their value). Non-unique secondary, text,
  fuzzy, geo, and vector indexes are also supported. The planner is used by
  `find`/`count`/`explain` and by indexed updates/deletes.
- `$out`/`$merge` write to time-series targets through the bucket writer, so
  indexes, events, and replication stay consistent.
- Aggregation streams row-local pipelines (`$match`, `$project`, `$addFields`,
  `$set`, `$unset`, `$replaceRoot`, `$lookup`, `$unwind`) in batches
  (`NONODB_TS_AGG_BATCH`, default 4096). A single `$group` stage streams into one
  builder; `$sort`/`$limit` then work on the group result. Pipelines with a
  blocking stage before `$group`, or `$facet`, use the materialized path.
- Point lookup by `_id` decodes one 256-row column block, not the containing
  bucket; deletes by `_id` use the locator directly.
- Open buckets are uncompressed until they seal.
- `bucketMaxDocs` bounds row count, not wall-clock rate; a very high ingest rate
  simply produces more buckets per window (serial counter in the key).
- Field names containing `.` collide with dotted nested paths when flattening.
- Arrays and `{"$date": ...}` objects are stored as single JSON values, not
  expanded into columns.
