# Columnar collections: Phase 0 feasibility report

Status: draft, experimental. This report records the Phase 0 benchmark and the
architecture decisions that feed into the
[columnar collections plan](columnar-collections-plan.md) and
[acceptance criteria](columnar-collections-acceptance.md).

No production profile or API was added. The benchmark is an example program and
uses the existing time-series profile as a storage proxy.

## 1. Question

Can the time-series bucket format become a general collection profile?

The specific questions were:

1. How much does a realistic user `_id` and its locator map cost?
2. How do point reads, updates, and deletes behave when documents are not
   time-series rows with synthetic ids?
3. Does changing bucket size fix the update/point-read gap?
4. Which partitioning and update strategy should the columnar profile use?

## 2. Environment

```text
Linux dev 7.0.14-29-pve x86_64
Intel(R) Xeon(R) D-2141I CPU @ 2.20GHz, 4 online CPUs in the container
rustc 1.100.0
cargo 1.100.0
release build
```

Reproduction:

```bash
ROWS=100000 SERIES=16 READS=500 \
  cargo run --release -p nonodb-core --example columnar_feasibility
```

Optional variables:

- `ROWS` — documents per scenario (default 100000)
- `SERIES` — number of device series (default 16)
- `READS` — point/update/delete sample size (default 500)
- `BUCKET_DOCS` — time-series bucket row limit (default 2048)

The example compares three scenarios:

- `standard+ids` — a standard collection with realistic UUID-sized `_id`s;
- `ts+user_ids` — the TS proxy with the same user `_id`s;
- `ts+synthetic_ids` — the TS proxy with generated synthetic ids, measuring
  the bucket format without the user-id column and locator map.

The generated documents have nested device metadata, arrays, optional fields
(schema drift), floats, counters, status strings, and booleans. Raw JSON is
341.36 B/row with ids and 298.36 B/row without ids. This is intentionally more
demanding than the 183 B/row time-series space benchmark.

## 3. Results at the default bucket size (2048)

| Scenario | Raw | Uncompacted | Compacted | Insert | Point read p50 | Update p50 | Delete p50 |
|---|---:|---:|---:|---:|---:|---:|---:|
| standard + user ids | 32.75 MB / 343.36 B/row | 43.98 MB / 461.13 B/row | 43.93 MB / 460.60 B/row | 2.70 s / 37.1k rows/s | 11.3 µs | 36.2 µs | 22.5 µs |
| TS proxy + user ids | 32.75 MB / 343.36 B/row | 35.47 MB / 371.96 B/row | **16.20 MB / 169.90 B/row** | 8.67 s / 11.5k rows/s | 770.6 µs | **873.8 ms** | **871.1 ms** |
| TS proxy + synthetic ids | 28.45 MB / 298.36 B/row | 18.30 MB / 191.94 B/row | **1.75 MB / 18.31 B/row** | 8.53 s / 11.7k rows/s | 604.8 µs | 12.35 ms | 13.35 ms |

Derived:

- the user-id column plus locator map costs **151.59 B/row** (14.46 MB for
  100k rows);
- with realistic user ids the TS proxy is only **2.71× smaller** than standard,
  not the 40× suggested by synthetic time-series rows;
- without user ids it is **25.16× smaller**;
- point reads are **68× slower** for user ids and **54× slower** for synthetic
  ids than standard;
- update/delete by a user `_id` takes **~874 ms** because the current TS update
  path scans the whole bucket set; it does not use the `0x05` id map for this
  path;
- even the synthetic direct-id update rewrites a bucket and takes **12.35 ms**
  versus 36 µs standard.

The update result is not a property of columnar storage itself. It exposes a
missing locator fast path and the cost of rewriting a 2048-row bucket.

## 4. Bucket-size tradeoff

The same workload with smaller buckets (`READS=100`):

| Bucket docs | User-id compacted | User point | User update | Synthetic compacted | Synthetic point | Synthetic update |
|---:|---:|---:|---:|---:|---:|---:|
| 2048 | 169.90 B/row | 770.6 µs | 873.8 ms | 18.31 B/row | 604.8 µs | 12.35 ms |
| 512 | 164.95 B/row | 752.2 µs | 842.7 ms | 18.64 B/row | 665.7 µs | 11.30 ms |
| 256 | 161.18 B/row | 773.4 µs | 738.1 ms | 20.03 B/row | 613.9 µs | 5.90 ms |
| 64 | 158.31 B/row | 276.3 µs | 769.2 ms | 66.72 B/row | 214.3 µs | 2.32 ms |

Smaller buckets improve synthetic update latency (12.35 ms → 2.32 ms) and
point read latency at 64 rows (604.8 µs → 214.3 µs), but increase storage
because bucket headers and field dictionaries are amortized over fewer rows.
They do **not** fix user-id update because the current path still scans all
buckets, and they still leave point reads far above the 3× standard gate.

The 64-row result is still roughly 61× slower for update and 17× slower for
point read than standard. Bucket size alone is not the answer.

## 5. Findings

### F1. The full canonical id map is too expensive for general documents

The current map key is `0x05 + id` and the value is a 27-byte locator. With
36-character UUID-like ids, the measured overhead is 141–152 B/row. It is a
large fraction of the total and turns a 25× storage win into a 2.7× win.

The map cannot be removed without a point-access story: a full scan is not
acceptable. The replacement must be compact and collision-safe.

### F2. Bucket rewrite is not an acceptable general update path

Even the synthetic direct-id update rewrites a bucket: 12.35 ms at 2048 rows,
5.90 ms at 256 rows, 2.32 ms at 64 rows. Standard is 36 µs. This is 61–342×
slower and would make ordinary document updates feel like batch operations.

A general profile needs an append-only update overlay or per-row version
records. Compaction can merge them into columnar buckets later.

### F3. Sealed point reads need finer random access

`decode_bucket_row` currently decodes all columns for the target 256-row block.
The measured point-read cost is 0.2–0.8 ms, 17–68× standard. Tiny buckets reduce
the block size but destroy compression; the format needs a sub-block or
row-addressable path for point reads.

### F4. Current TS update code must not be reused for user ids

`find_by_id` uses the id map, but the update path does not derive a direct
locator for user-supplied ids. It scans all bucket payloads. The general
profile must make the locator authoritative for find, update, delete,
`findAndModify`, TTL, and replication.

### F5. Storage claims must include realistic ids and indexes

The 10.69 B/row time-series number is real for synthetic ids and no user id
column. It is not a general-document number. Phase 1 benchmarks must report the
user-id map, indexes, history, and schema drift separately.

## 6. Decisions

### D1. Partitioning and id access

Use a **compact hash locator** as the primary point-access directory, not the
full canonical `_id` string as the map key.

Reference shape:

```text
hash64(_id) -> compact locator(s)
```

The locator identifies the bucket and row. The full `_id` remains in the
document columns and is verified after a lookup. Hash collisions store a short
collision chain and are resolved by comparing the actual `_id`.

Buckets are append segments of a documented target size (working default:
512 rows). The directory is a storage accounting item and is included in
compaction, backup, and replication.

The full canonical id map may remain as a compatibility/debug fallback, but it
must not be the default representation for general documents.

### D2. Update strategy

Use an **append-only hot overlay**, not a whole-bucket rewrite, for sealed rows:

- an update writes a new version record plus index/history/event changes in the
  same transaction;
- reads check the overlay before decoding the base bucket;
- deletes write a tombstone and remove the active locator;
- a background/seal/compaction step merges overlays into a new columnar bucket;
- open-bucket rows can still be mutated directly.

The overlay must be bounded and compacted; it may not grow without limit. This
is the only way to approach standard update latency without giving up columnar
storage for append-heavy data.

### D3. Point-read access

Add a sub-block or row-addressable index for sealed buckets. The default
256-row block remains the compression unit, but point reads must not decode all
columns of a full 256-row block.

Working target: a sealed point read should decode only the target document
column values and a small surrounding sub-block. The exact sub-block size is a
Phase 1 benchmark decision.

### D4. Default bucket size

Retain a larger bucket target (working default 512) for compression. The
overlay removes the update penalty that previously made small buckets tempting.
If sub-block access still cannot meet the point-read gate, reduce the bucket
target only for the point-read path or add a row cache; do not globally shrink
buckets and silently lose the storage win.

### D5. Id-map performance gate

Add a hard gate: update/delete by `_id` must use the locator and must not scan
the collection. A profile that requires a full bucket-set scan for a point
mutation is not acceptable, even if its storage is good.

## 7. Consequences for Phase 1

Phase 1 must implement and test these before broad query work:

1. compact 64-bit hash locator with collision handling;
2. overlay update/delete path with read-your-writes;
3. point read that checks the overlay and decodes only the target row/sub-block;
4. compaction that merges overlay records without changing visible documents;
5. storage accounting for the locator, overlay, base buckets, indexes, and
   history;
6. differential tests for find/update/delete/findAndModify after seal,
   compaction, restart, and replication.

The current Phase 0 proxy is not sufficient for update/point-read parity. It is
sufficient to prove that storage wins are real and to reject three tempting but
wrong designs: a full string id map, a whole-bucket update path, and
bucket-size tuning alone.

## 8. Remaining questions

- Exact hash collision representation and expected false-positive rate.
- Sub-block size and whether it is a format field or a per-collection setting.
- Overlay format: JSON rows, delta documents, or columnar mini-buckets.
- Overlay compaction threshold and interaction with online compaction.
- Whether point reads should use a small row cache for hot documents.
- Whether history entries can share the overlay's version records.
