# NonoDB time-series vs TimescaleDB — full benchmark

This document collects, in one place, the environment, method, query
definitions, results, storage, index sizes, RAM usage, correctness checks and
caveats for the comparison between NonoDB (`profile: "timeseries"`) and
TimescaleDB on a real 206-million-row dataset.


## 1. Environment

| Item | NonoDB | TimescaleDB |
|---|---|---|
| CPU | Intel Xeon D-2141I @ 2.20 GHz, 4 vCPU visible to the process | same host, 4 vCPU visible in the container |
| RAM | 14 GiB in the test environment, ~11 GiB available | `pg` container limited to **6 GiB**; CT 103 has 8 GiB |
| System | Linux, in-process | PostgreSQL **17.6**, TimescaleDB **2.21.3**, Alpine |
| Version | NonoDB 0.32.0, Rust 1.100.0, release build | official TimescaleDB image |
| Settings | `NONODB_MAP_SIZE=64GiB`, otherwise defaults | `shared_buffers=2GB`, `work_mem=128MB`, `max_connections=25`, `synchronous_commit=off` |
| Compression | columnar NTB2/v3 buckets, per-column codecs | native TimescaleDB compression |
| Indexes | bucket locators, posting lists, partial `symbol_ts_recent` | B-tree indexes, including `symbol_ts_recent` |

Both processes see 4 vCPU. NonoDB ran as a single process without a network
client; TimescaleDB was queried through `psycopg2` over the LAN.

## 2. Dataset

`ohlc_data` table/collection:

| Field | Type / meaning |
|---|---|
| `time` / `ts` | timestamp; data ends at `2026-11-26T08:39:00Z` (`END_MS=1795682340000`) |
| `symbol` | series, e.g. `BTC/USD`; hundreds of symbols (429 series in the index) |
| `exchange`, `interval`, `source` | metadata |
| `open`, `high`, `low`, `close`, `volume` | OHLCV values |

| Metric | NonoDB | TimescaleDB |
|---|---:|---:|
| Rows | **206,336,608** | **206,345,595** |
| Difference | — | +8,987 rows (**0.004%**); snapshots differ slightly |
| Database size | **7,881,351,168 B = 7.34 GiB** (`data.mdb`) | **11.07 GiB** (11.89 GB, `pg_database_size('collect_data')`) |
| `symbol_ts_recent` index | **~52.3 MB** | **~435 MB** (`pg_indexes_size` + chunks) |
| Buckets / chunks | 375,522 sealed + 429 open | hypertable with native compression |

Queries on both sides use the same end timestamp. The 8,987-row difference is
0.004% of the dataset and does not change the order of magnitude of the
results.

## 3. Method

- NonoDB: `ts-nonodb-tool bench`, one warm-up run not reported, then **3 runs
  and the median**. The time covers query execution and result processing in
  the process, with no network.
- TimescaleDB: a separate `psycopg2` connection, one warm-up execution, then
  **3 runs and the median**. The time covers server execution plus sending and
  receiving the result over the LAN.
- NonoDB aggregations use materialized 1h/1d rollups; TimescaleDB in this
  measurement runs plain queries against the raw, compressed hypertable
  (without a continuous aggregate). This is an important caveat: TimescaleDB
  with a continuous aggregate could be faster for these aggregations.
- All times are in milliseconds. "Winner" means the lower median time.

## 4. Query definitions

| Label | NonoDB | TimescaleDB |
|---|---|---|
| count last minute | `count({symbol:"BTC/USD", ts: [end-60s, end]})` | `count(*) WHERE symbol='BTC/USD' AND time BETWEEN ...` |
| count 1 h | `count({symbol:"BTC/USD", ts: [end-1h, end]})` | same, 1 h window |
| count 1 d | `count({symbol:"BTC/USD", ts: [end-1d, end]})` | same, 1 d window |
| find 1 d, limit 100 | `find(filter, sort:{ts:-1}, limit:100)` | `SELECT * ... ORDER BY time DESC LIMIT 100` |
| OHLC BTC 30 d | `$match` symbol + 30 d, `$sort ts`, `$group` by `$dateTrunc(ts,"hour")`, `$first open`, `$max high`, `$min low`, `$last close`, `$sum volume`, `$sum 1` | `time_bucket('1 hour', time)`, `first(open,time)`, `max(high)`, `min(low)`, `last(close,time)`, `sum(volume)`, `count(*)` |
| OHLC all symbols 1 h | filter 1 h, `$group` by `symbol` + hour, `$count` groups | `GROUP BY symbol, time_bucket('1 hour', time)` |
| OHLC all symbols 1 d | filter 1 d, `$group` by `symbol` + day, `$count` groups | `GROUP BY symbol, time_bucket('1 day', time)` |

Note: the label "count last minute" counts rows in a one-minute window; it
does not fetch a document by `_id`.

## 5. Final results

Medians of 3 runs on the full 206-million-row dataset:

| Operation | NonoDB | TimescaleDB | Winner and advantage |
|---|---:|---:|---|
| count BTC/USD, last 1 minute | **1.08 ms** | 4.14 ms | **NonoDB 3.8× faster** |
| count BTC/USD, 1 h | **1.04 ms** | 3.83 ms | **NonoDB 3.7× faster** |
| count BTC/USD, 1 d | **2.78 ms** | 5.58 ms | **NonoDB 2.0× faster** |
| find BTC/USD, 1 d, limit 100, sort desc | **6.13 ms** | 6.58 ms | **NonoDB 1.07× faster** |
| OHLC 1 h BTC/USD, 30 d | **5.52 ms** | 56.80 ms | **NonoDB 10.3× faster** |
| OHLC 1 h all symbols, 1 h | 16.79 ms | **2.49 ms** | **TimescaleDB 6.75× faster** |
| OHLC 1 d all symbols, 1 d | 65.25 ms | **33.63 ms** | **TimescaleDB 1.94× faster** |
| RAM: process vs container | **26.1 MiB** process RSS | **~2.2 GiB** cgroup | **NonoDB ~85× less RAM** |
| Database size | **7.34 GiB** | 11.07 GiB | **NonoDB 1.51× smaller** |
| `symbol_ts_recent` index | **~52.3 MB** | ~435 MB | **NonoDB 8.3× smaller** |

\* RAM is not directly comparable: the NonoDB number is process RSS only
(OS page cache is additional), while the TimescaleDB number is the whole `pg`
cgroup including `shared_buffers`/`shmem`. See section 6 for the detailed
breakdown.

Key numbers:

- NonoDB wins every single-series query: **3.8× faster** for count last minute,
  **3.7×** for count 1 h, **2.0×** for count 1 d, **1.07×** for find 1 d with
  limit 100 and **10.3×** for OHLC BTC 30 d.
- TimescaleDB wins the all-series aggregations: **6.75× faster** for OHLC all
  symbols 1 h and **1.94×** for OHLC all symbols 1 d.
- NonoDB uses **~85× less process RAM**, is **1.51× smaller on disk** and has
  an **8.3× smaller** `symbol_ts_recent` index.
- Stage targets: BTC 30 d **≤150 ms** (actual 5.52 ms), all symbols 1 d
  **≤300 ms** (actual 65.25 ms) — met.

## 6. RAM usage

NonoDB was sampled through `/proc/<pid>/status` and `/proc/<pid>/smaps_rollup`
during the benchmark; TimescaleDB through `/sys/fs/cgroup/memory.current` and
`memory.stat` of the `pg` container.

| Metric | NonoDB | TimescaleDB |
|---|---:|---:|
| Peak observed memory | **26.1 MiB** process RSS (**24.8 MiB** PSS) | **~2.18–2.23 GiB** cgroup `memory.current` |
| Shared cache | no explicit shared buffer; LMDB `mmap` is backed by OS page cache | **~2.07 GiB** `shmem` (`shared_buffers=2GB`; `pg_shmem_allocations` ≈2.19 GB) |
| Private/anonymous memory | ~26 MiB in the measured process | **~38–88 MiB** cgroup `anon`; individual backend contexts are much smaller |
| File/page cache | outside process RSS; OS-managed LMDB pages | cgroup `file` **~2.13 GiB**, mostly the same `shmem` |
| Virtual mapping | **64 GiB** LMDB `mmap` (not resident) | **~2.2 GiB** shared-memory mapping per backend |
| Hard memory limit | none in the benchmark (14 GiB environment) | **6 GiB** cgroup (CT 103 has 8 GiB) |
| Per-query memory budget | no `work_mem`; process peak stayed ~26 MiB | `work_mem` **128 MB** per operation |
| Connection limit | no server connection pool in this test | `max_connections=25` |

Conclusion: **NonoDB has ~26 MiB of process overhead**, but the OS page cache
for LMDB pages read by queries is additional. **TimescaleDB actually uses
~2.2 GiB**, mostly because of `shared_buffers=2GB`; a single backend has only a
few MiB of private memory, so that number must not be confused with total
server memory. Under heavy sorting/hashing TimescaleDB can grow toward
`work_mem × concurrent queries`, up to the 6 GiB limit.

## 7. Indexes and size

| Item | NonoDB | TimescaleDB |
|---|---:|---:|
| Database | 7.34 GiB | 11.07 GiB |
| `symbol_ts_recent` index | ~52.3 MB | ~435 MB |
| Bucket locator entries | 751,902 (375,951 buckets × 2) | — |
| Rollups | 11,685 entries, +2.17 MiB | — |
| Bucket format | NTB2 / v3 (zone map + Bloom) | native chunks |

NonoDB stores series metadata once per bucket and secondary indexes as
per-bucket posting lists. TimescaleDB has a larger index footprint but wins on
columnar execution for aggregations across all series.

## 8. Correctness and tests

- `rollups_match_raw_aggregations_and_invalidate_on_write` — 1h/1d rollups
  compared with raw aggregation; insert updates a rollup, delete invalidates it.
- `bucket_locator_indexes_follow_insert_delete_and_retention` — locators after
  insert/seal/delete/retention.
- `indexed_count_and_ordered_find_match_natural_scan` — count and ordered find
  match the natural scan.
- `zone_maps_prune_without_decoding` — zone-map/Bloom pruning without decoding.
- Time-series unique-index tests, including `(locator, seq)`.
- `cargo test --workspace` — **0 failures**.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo fmt --all -- --check` — clean.

## 9. Reproduction

```bash
# NonoDB — full 206M-row benchmark
cd /home/docker/nono-fork-rob
NONODB_MAP_SIZE=64GiB END_MS=1795682340000 \
  /tmp/ts-nonodb-tool/target/release/ts-nonodb-tool \
  bench data/timescale_collect_data ohlc_data

# rollups (rebuild after writes if needed)
NONODB_MAP_SIZE=64GiB END_MS=1795682340000 \
  /tmp/ts-nonodb-tool/target/release/ts-nonodb-tool \
  rollup data/timescale_collect_data ohlc_data

# window functions (isolated runs; see section 10)
NONODB_MAP_SIZE=64GiB END_MS=1795682340000 \
  /tmp/ts-nonodb-tool/target/release/ts-nonodb-tool \
  window data/timescale_collect_data ohlc_data 7
```

TimescaleDB: the same SQL queries from section 4 against the `collect_data`
database (`TIMESCALE_URL` in `timescaledb.env`).

## 10. Window functions (`$setWindowFields`)

Window execution was measured on the same 206M-row import with the same end
timestamp. NonoDB runs in-process; TimescaleDB queries run through `psycopg2`
over the LAN and fetch the full result set. Both sides partition by `symbol`
and order by `time`; TimescaleDB uses `RANK() OVER (...)`,
`AVG(close) OVER (... ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)` for the
60-row average, and `AVG(close) OVER (... RANGE BETWEEN INTERVAL '1 hour'
PRECEDING AND CURRENT ROW)` for the 1-hour range. The times cover query
execution and result processing.

| Case | Rows | NonoDB | TimescaleDB | Winner and advantage |
|---|---:|---:|---:|---|
| `$rank`, BTC/USD 7 d | 10,081 | 71 ms | 71.3 ms | tie (1.00×) |
| 60-row `$avg close`, BTC/USD 7 d | 10,081 | **100 ms** | 163.3 ms | **NonoDB 1.63× faster** |
| 1-hour range `$avg close`, BTC/USD 7 d | 10,081 | **91 ms** | 168.9 ms | **NonoDB 1.86× faster** |
| `$rank`, all symbols, last 1 h | 1,155 | 11 ms | **8.3 ms** | **TimescaleDB 1.33× faster** |
| 60-row `$avg close`, all symbols, last 1 h | 1,155 | 12 ms | **8.6 ms** | **TimescaleDB 1.39× faster** |
| 1-hour range `$avg close`, all symbols, last 1 h | 1,155 | 12 ms | **8.1 ms** | **TimescaleDB 1.48× faster** |

The all-symbols cases decode 38 open buckets and 1,155 rows. The columnar scan
reads only the time field before materializing a row, so rows outside the
requested window are skipped without allocating a full JSON document.

NonoDB wins the single-series moving and range frames; the all-symbols cases
favor TimescaleDB's columnar executor and result path. Correctness coverage
includes `grouped_unbounded_prefix_matches_brute_force`,
`parallel_partitions_match_brute_force`,
`window_scan_matches_find_for_time_ranges`, and
`fast_float_is_opt_in_and_matches_within_tolerance`; differential window tests
compare against the brute-force reference.

## 11. Comparison caveats

- NonoDB is measured in-process; TimescaleDB is measured over the network and
  through a client, so the timings cover different layers.
- NonoDB uses rollups for OHLC; TimescaleDB in this measurement does not use a
  continuous aggregate.
- "All symbols" aggregations show the advantage of TimescaleDB's columnar
  executor; single-series reads and counts are won by NonoDB.
- OS page cache is not included in NonoDB process RSS, while `shmem` is included
  in the TimescaleDB cgroup — RAM must be compared with those layer differences
  in mind, not as a single number.
