# nonodb

[![CI](https://github.com/essmos/nonodb/actions/workflows/ci.yml/badge.svg)](https://github.com/essmos/nonodb/actions/workflows/ci.yml)
[![Release](https://github.com/essmos/nonodb/actions/workflows/release.yml/badge.svg)](https://github.com/essmos/nonodb/actions/workflows/release.yml)

**A low-resource, crash-safe document database for application state,
settings, and JSON-heavy configuration.**

nonodb is a local NoSQL document store written in Rust. It needs no external
database server, keeps a tiny idle memory footprint, and runs comfortably on
small VMs, edge boxes, and developer machines. It speaks a practical subset of
MongoDB's query language over HTTP/JSON, stores documents in LMDB, and runs as
a single daemon that is the only process allowed to touch the database file.
Application workers connect to it through a Unix socket and use a thin client.

If you need a shared, queryable JSON store without operating PostgreSQL,
MongoDB, or Redis, nonodb is aimed at you.

```text
┌──────────────────┐
│ uvicorn worker 1 │──┐
│ uvicorn worker 2 │──┤   HTTP/JSON over Unix socket   ┌───────────────┐
│ ...              │──┼───────────────────────────────▶│ nonodb-daemon │
│ uvicorn worker N │──┘                                │ 1 writer      │
└──────────────────┘                                   │ 1 LMDB env    │
                                                       └───────┬───────┘
                                                               │
                                                         ./data (LMDB)
```

## Why nonodb exists

Web applications accumulate small pieces of shared state: feature flags,
tenant settings, notification templates, routing rules, counters, cached
metadata, user preferences, and other JSON-shaped records. At first these live
in YAML files or environment variables. Sooner or later several processes need
to read and update them concurrently, query them by more than one field, and
survive a crash.

The usual answers are heavier than the problem:

- **PostgreSQL / MySQL** require a server, a schema, migrations, connection
  pooling, and operational work for what is often a handful of collections.
- **MongoDB** is a full distributed database with its own deployment and
  security surface.
- **Redis** is excellent for keys and counters, but it is not a document store
  with secondary indexes, rich filters, or durable snapshots by default.
- **Files on disk** are easy to start with and painful to keep consistent:
  partial writes, no concurrent update semantics, no indexes, no atomic batch.
- **An embedded database inside every worker** looks attractive, but LMDB and
  many embedded engines are sensitive to `fork()`, multiple owners, and stale
  readers. With preforking servers such as Gunicorn or uvicorn, every worker
  would otherwise open the same environment independently.

nonodb takes the SQLite philosophy — embedded, zero-administration, one local
file — and applies it to JSON documents. It separates the database engine from
its clients:

1. **One daemon owns the LMDB environment.** No other process opens the data
   file. This removes multi-process mmap, `fork()`, and stale-reader problems.
2. **Clients are thin.** Application workers talk HTTP/JSON over a Unix socket.
   A Python client wraps this in a small, familiar API.
3. **Writes are serialized and grouped.** One writer thread batches requests and
   performs one `fsync` per group, while reads are concurrent MVCC snapshots.
4. **Query semantics are familiar.** Filters, projections, sorts, secondary
   indexes, update operators, and upserts follow MongoDB conventions where it
   makes sense.
5. **The storage engine is boring on purpose.** LMDB is copy-on-write,
   memory-mapped, and crash-safe. A `kill -9` does not corrupt the database.

The result is a small service that feels like an embedded database to the
application, but behaves like a shared database to a preforking web server.

## What nonodb is not

nonodb is deliberately not a general-purpose distributed database. It has:

- no sharding, replication, or clustering;
- no general interactive multi-document transactions;
- no MongoDB wire-protocol compatibility;
- no aggregation framework, `$lookup`, or TTL indexes;
- no user/role system beyond a bearer token;
- a practical subset of MongoDB query and update operators.

For a single host, a small-to-medium data set, and a workload dominated by
indexed reads and modest writes, those trade-offs buy a very small operational
footprint.

## Features

### Documents and queries

- JSON documents in named collections, stored as JSON on disk.
- Automatic `_id` generation with UUID v4; custom string `_id` is supported.
- MongoDB-style filters: equality, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`,
  `$in`, `$nin`, `$exists`, `$not`, `$and`, `$or`, `$nor`.
- Nested paths (`{"user.address.city": "..."}`), array membership, and
  consistent numeric comparison (`30 == 30.0`).
- Projection, multi-field sort, `skip`, and `limit`.
- Fast `find_one`/`find` paths that stop after enough matches, and `count({})`
  in O(1).

### Writes and updates

- `insert`, `insert_many`, `update`, `delete`, with optional `multi`.
- Update operators: `$set`, `$unset`, `$inc`, `$push`, `$pull`, `$addToSet`,
  `$setOnInsert`.
- `$each` for `$push` and `$addToSet`.
- Upsert with filter equality seeding, replacement upserts, and
  `$setOnInsert`.
- Replacement updates preserve `_id`.
- Atomic `write_batch`: all operations commit or the whole batch rolls back.
- Read-only `batch`: many reads in one LMDB snapshot and one HTTP round trip.

### Indexes

- Single-field and compound secondary indexes.
- Unique indexes, enforced on insert, update, and index backfill.
- Order-preserving key encoding for ranges.
- Index-only sorted scans when a sort matches the full index order.
- Covering indexes with `include`, serving projections without fetching the
  document.
- Index-backed `count` when the filter exactly matches the indexed fields.
- Parallel index backfill for existing collections.

### Search

- Full-text search with an inverted index and BM25 ranking (`_score`).
- Unicode-aware tokenization, lowercase, and diacritic folding
  (`zażółć` → `zazolc`).
- Quoted phrases requiring contiguous token sequences.
- Fuzzy search with trigrams plus bounded Levenshtein distance (`_distance`).
- Both search indexes are maintained in the same transaction as the document.

### Operations

- Single daemon; Unix socket by default, optional TCP.
- Optional bearer-token authentication and Unix peer uid/gid allow-lists.
- Optional TLS for TCP using rustls.
- Periodic LMDB snapshots with retention.
- Online backup with compaction; CLI restore and inspection.
- JSON metrics endpoint and request/queue/commit counters.
- Group commit with adaptive batching, request size limits, and request
  timeouts.
- Docker image, systemd unit, crash tests, and end-to-end test scripts.

## Quick start

Requirements:

- Rust stable (2021 edition; latest stable recommended) and Cargo.
- Linux for the daemon (Unix sockets and `SO_PEERCRED` checks are Linux/Unix
  features).
- Python 3.9+ for the optional client (`httpx`; `orjson` is an optional speedup).

Build:

```bash
cargo build --release
```

Run the daemon:

```bash
mkdir -p ./data
NONODB_DATA=./data NONODB_SOCK=/tmp/nonodb.sock ./target/release/nonodb-daemon
```

Insert and query with `curl`:

```bash
S=/tmp/nonodb.sock

curl -s --unix-socket "$S" -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"insert","coll":"users","doc":{"name":"Ada","age":36}}'

curl -s --unix-socket "$S" -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"find","coll":"users","filter":{"age":{"$gte":18}},"sort":[["age",-1]]}'
```

Use the Python client:

```bash
export PYTHONPATH="$PWD/client"
python3 - <<'PY'
from nonodb import NonoDB

with NonoDB("/tmp/nonodb.sock") as db:
    users = db.collection("users")
    users.insert({"name": "Grace", "age": 41})
    print(users.find({"age": {"$gte": 18}}, sort=[("age", -1)]))
PY
```

Use the CLI:

```bash
NONODB_SOCK=/tmp/nonodb.sock ./target/release/nonodb-cli ping
NONODB_SOCK=/tmp/nonodb.sock ./target/release/nonodb-cli count users '{}'
NONODB_SOCK=/tmp/nonodb.sock ./target/release/nonodb-cli find users '{}' --sort age:desc --limit 5
```

## Documentation

| Document | What it covers |
|---|---|
| [Architecture](docs/architecture.md) | Daemon model, LMDB layout, write path, indexes, search, concurrency, crash safety |
| [HTTP API](docs/api.md) | Every operation, request/response shapes, filters, updates, indexes, search, errors |
| [Python client](docs/python-client.md) | `NonoDB`/`Collection` API, caching, retries, TCP/TLS, FastAPI usage |
| [CLI](docs/cli.md) | `ping`, `find`, `count`, `backup`, `restore`, `inspect`, `exec` |
| [Operations](docs/operations.md) | Configuration, deployment, security, snapshots, monitoring, capacity, troubleshooting |
| [Benchmarks](docs/benchmarks.md) | Test environment, throughput, latency, large data sets, RAM, reproducibility |
| [Development](docs/development.md) | Repository layout, tests, linting, benchmark scripts, contribution notes |
| [Roadmap and limits](docs/roadmap.md) | Deliberate limitations, current non-goals, future work |

## At a glance

Measured on an 8 vCPU Haswell host, ext4, release build, 12 client processes.
See [Benchmarks](docs/benchmarks.md) for methodology and full tables.

| Workload | Result |
|---|---|
| Concurrent inserts, 12 clients | ~3.3k ops/s, p50 3.2 ms |
| `find_one` by `_id`, 12 clients | ~6.8k ops/s, p50 1.3 ms |
| Indexed lookup at 1M / 2M / 4M / 8M docs | 0.5–1.5 ms p50 |
| `count({})` at 8M docs | ~0.6 ms |
| 1,000 `find_one` calls in read batches | ~20 ms (~50k ops/s) |
| Index-only sort at 1M docs | 1–2 ms |
| Full sort at 1M docs (no index) | 250–420 ms |
| Idle daemon RSS at 1M docs, 3 indexes | ~5 MB |
| Live backup of 8M docs + 1M text docs | 5.1 GB in ~10 s |

## Status

nonodb is an early-stage project (`0.1.0`). Its core storage, query, index,
search, batching, security, snapshot, and operational paths are covered by unit,
property-based, end-to-end, concurrency, crash, and large-data tests. The API
may still change before a stable release.

## License

MIT. See [LICENSE](LICENSE).
