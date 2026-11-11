# Test results

The full benchmark and test report is maintained in
**[docs/benchmarks.md](docs/benchmarks.md)**. This page keeps the high-level
status and points to the detailed methodology and tables, so there is one source
of truth for performance numbers.

## Latest status

Measured on 2026-11-11 (UTC) on an 8 vCPU Haswell host, 22.9 GiB RAM, ext4,
release build. The latest validation also exercised a 10M-document collection,
500k text documents, and a full backup/restore cycle.

| Check | Result |
|---|---|
| `cargo build --release --examples` | OK |
| `cargo fmt --check` | OK |
| `cargo clippy --all-targets -- -D warnings` | 0 warnings |
| `cargo test --workspace --all-targets --all-features` | 242 tests passing |
| CRUD, cache, batch, write batch, upsert, search, compound indexes | OK |
| 12-process concurrency and unique-index race tests | OK |
| `kill -9` during writes | No corruption; committed data readable |
| 12 uvicorn workers against one daemon | OK |
| Live backup and restore, 1M, 8M, and 10M documents | OK |
| Docker build and named-volume run | OK |
| Vault encryption regression suite | OK (profiles, headers, isolation, CRUD, history, rejected ops) |
| Auth registry, scopes, and lockout | OK (tokens, scopes, filtered list, live reload, audit, per-IP blocks) |
| mTLS with all official clients | OK (Python, Rust, Node.js, Go; anonymous and revoked certs rejected) |
| Document TTL suite | OK (defaults, overrides, update semantics, sweeper, history tombstones/purge, vault) |
| 10M-document load and 3 index backfills | OK (157.8 s load; 75.5 s indexes) |
| 10M-document backup, verify, restore, and inspect | OK (6.7 GB backup; 52 s backup; 2m38s verify; 2m52s restore+verify) |
| 1M typed search/aggregation matrix | OK (10 indexes, 200k text docs; typed lookups 0.6–1.2 ms) |
| Planner and cursor regression | OK (sort-only index, `$or` union, `$type` ranges, cursor paging/ownership) |
| Multikey and aggregation regression | OK (array keys, unique multikey, min/max sort, `$all`/`$elemMatch` plans, top-K sort, streaming `$group`) |
| 10k keepalive mixed workload | OK (5 min UDS at 4.9k req/s; 2 min TLS at 4.1k req/s; 0 errors) |

## Large-scale and connection-storm run (2026-11-11)

Host: 8 vCPU, 22.9 GiB RAM, ext4. Release daemon, `NONODB_CACHE_TTL=0`,
`ulimit -n 65536`, fresh temporary databases. Both daemons were stopped and the
temporary data was removed after the run.

### Large data and multikey indexes

| Workload | Result |
|---|---|
| 1M documents, 242 MB raw data | 12.4 s load, ~80,900 docs/s |
| 3 scalar indexes on 1M documents | 6.1 s backfill; data file 611 MB |
| 1M multikey documents, 3 tags + unique array | 16.2 s load, ~61,800 docs/s |
| Multikey `tags` index, 3M entries | 6.0 s backfill |
| Unique multikey `uq` index, 1M entries | 3.5 s backfill |
| `count(tags = x)` on 1M docs | 1.25 ms p50 / 1.60 ms p95 |
| `find(tags = x, limit 10)` | 0.71 ms p50 / 0.87 ms p95 |
| `count(tags: {$all: [a,b,c]})` | 1.19 ms p50 / 1.72 ms p95 |
| Sort by `tags` asc/desc, limit 10 | 0.72 / 0.80 ms p50 |
| Unique multikey collision | rejected with `duplicate_key` |
| `$group` over 1M docs into 1,000 groups | 1.86 s |
| Top-K `$sort` by unindexed field + `limit 10`, 1M docs | 1.66 s |

The multikey sort used `index_scan` with `sort_covered=true`. The multikey
indexes reported `multikey_fields=['tags']` / `['uq']` and
`multikey_all_docs=true`.

### 1,000 and 10,000 simultaneous clients

Each client opened a socket, held it open for about 2 seconds, then sent one
`list_collections` request and read the response. All sockets were open
simultaneously before the request phase.

| Transport | Clients | Connect result | Connect time | Requests | Request time | Server fds at peak |
|---|---:|---|---:|---|---:|---:|
| Unix socket | 1,000 | 1,000 OK / 0 failed | 0.078 s | 1,000 OK / 0 failed | 0.186 s | 1,013 |
| Unix socket | 10,000 | 10,000 OK / 0 failed | 1.107 s | 10,000 OK / 0 failed | 1.056 s | 10,013 |
| TCP + bearer auth | 1,000 | 1,000 OK / 0 failed | 1.141 s | 1,000 OK / 0 failed | 0.255 s | 1,014 |
| TCP + bearer auth | 10,000 | 10,000 OK / 0 failed | 2.189 s | 10,000 OK / 0 failed | 1.511 s | 10,014 |

After both phases the daemon returned to 13–14 open fds, with no leaked
connection tasks. Peak RSS was about 1.40 GB for the UDS test against the loaded
1M+1M document database and about 201 MB for the TCP test against an empty
database. The 10,000-client UDS and TCP runs each completed with zero HTTP
errors and no daemon warnings or errors in the logs. TLS handshakes were not
included in this storm test; the sustained keepalive run below covers TLS.

## 10k keepalive mixed workload (2026-11-11)

This run kept all 10,000 connections open and sent a mixed workload for a
sustained period. The dataset was 200,000 documents with an index on `grp`,
one hot document for updates, and a separate write collection. The daemon used
`NONODB_CACHE_TTL=0`. Operations were weighted: 50% `find_one` by `_id`, 25%
indexed `find` by `grp` (limit 10), 13% indexed `count`, 5% bounded aggregate
(`$match` → `$limit 10` → `$group`), 4% insert, and 3% `$inc` on the hot
document.

| Run | Clients | Duration | OK requests | Errors | Throughput | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| UDS keepalive | 10,000 | 300 s | 1,491,594 | 0 | 4,910 req/s | 0.49 ms | 12.5 ms | 73.4 ms |
| TCP + TLS keepalive | 1,000 | 120 s | 59,940 | 0 | 483 req/s | 0.50 ms | 2.19 ms | 3.03 ms |
| TCP + TLS keepalive | 10,000 | 120 s | 582,978 | 0 | 4,148 req/s | 1.93 ms | 132 ms | 238 ms |

Details:

- UDS: 10,013 server fds stable for the whole run, RSS 493–507 MB, server CPU
  78–84% of one core, client CPU ~50% of one core. After close: 14 fds.
- TLS 10k: handshake/open phase took 17.7 s (~564 new TLS connections/s),
  10,014 fds, server RSS 334–359 MB, server CPU 88–97% of one core. The Python
  TLS client used ~3.3 GB RSS, so the client process was the memory bottleneck,
  not the daemon.
- TLS 1k: 1,000/1,000 connections opened in 1.6 s; server RSS 79–88 MB, CPU
  14–15% of one core.
- The 10k TLS run completed with 0 failed connections and 0 request errors.
  After close the daemon returned to 14 fds. Its log contained no warnings or
  errors.

### Important aggregation finding

An unbounded `$match(grp)` → `$group` on the 200k collection took ~135–141 ms
per query even though only 1,000 documents matched. With that stage in 5% of
the mix, throughput collapsed to ~0.5k req/s and p50 latency rose to seconds.
Adding `$limit 10` before `$group`, or using `$count`, reduced the same
aggregation to 0.6–0.8 ms. The sustained 10k result above uses the bounded
form. The planner should be checked for missing index pushdown / early
termination on unbounded `$match` → `$group`.

## Representative numbers

| Workload | Result |
|---|---|
| 12-client inserts, ext4 | ~3,334 ops/s, p50 3.21 ms |
| 12-client `find_one` by `_id` | ~6,833 ops/s, p50 1.30 ms |
| 1M inserts | 13.3–13.6 s (~74k–75k docs/s) |
| 3 index backfills, 1M docs | 3.4–3.7 s |
| Indexed lookup, 1M–8M docs | 0.5–1.5 ms p50 |
| `count({})`, 8M docs | ~0.60 ms |
| 1,000 `find_one` in read batches | ~19–23 ms |
| Index-only sort, 1M docs | 1–2 ms |
| Full sort, 1M docs | 250–420 ms |
| Idle RSS, 1M docs with 3 indexes | ~5.1 MB |
| Live backup, 8M docs + 1M text docs | 5.1 GB in ~10 s |
| Vault read by `_id`, 5k docs | 1.32 ms p50 (1.36x standard) |
| Vault insert, 5k docs | 2.56k docs/s (2.79x standard) |
| 10M inserts | 157.8 s (~63.4k docs/s) |
| 3 index backfills, 10M docs | 75.5 s |
| 10M-document data file, 3 indexes | ~6.2 GB |
| 10M-document backup | 6.7 GB in 52.4 s |
| 10M-document verify-backup | 2m38s, ~6.9 GB peak RSS |
| 10M-document restore + verify | 2m52s, ~6.7 GB peak RSS |

Detailed tables, caveats, and reproduction commands:
[docs/benchmarks.md](docs/benchmarks.md).
