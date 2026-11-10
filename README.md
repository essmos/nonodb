# NoNoDB

[![CI](https://github.com/essmos/nonodb/actions/workflows/ci.yml/badge.svg)](https://github.com/essmos/nonodb/actions/workflows/ci.yml)
[![Release](https://github.com/essmos/nonodb/actions/workflows/release.yml/badge.svg)](https://github.com/essmos/nonodb/actions/workflows/release.yml)

**A low-resource, crash-safe JSON document database — with a surprisingly
large feature set under the hood.**

**Idle daemon memory: about 5 MB RAM.** That is measured with a 1M-document
database and three indexes after a restart. The on-disk LMDB file grows with
the data and indexes; the ~5 MB figure is process memory, not the database
file. Hot data stays in the OS page cache instead of the daemon heap.

NoNoDB is a local NoSQL document store written in Rust. It needs no external
database server and runs comfortably on small VMs, edge boxes, and developer
machines. It speaks a practical subset of MongoDB's query language over
HTTP/JSON, stores documents in LMDB, and runs as a single daemon that is the
only process allowed to touch the database file. Application workers connect
to it through a Unix socket and use a thin client.

Low resource use is the starting point, not the whole story. Under the hood,
nonodb is a real document database with:

- JSON documents, MongoDB-style filters, projections, sorting and updates;
- single-field, compound, unique and covering secondary indexes;
- atomic `write_batch` for all-or-nothing groups of writes;
- full-text search with **BM25** ranking, Unicode tokenization, diacritic
  folding and quoted phrases;
- fuzzy search using trigrams plus Levenshtein distance;
- a read-only aggregation pipeline for reporting and summaries;
- snapshots, online backup, authentication, TLS/mTLS, metrics and a CLI;
- document revision history with global revision IDs and configurable retention;
- document TTL with collection defaults, per-document overrides, and background expiry;
- optional vault collections with whole-document encryption and per-tenant keys;
- a bounded daemon read cache, invalidated after every committed write;
- optional automatic indexes for frequent query shapes, with TTL cleanup.

**Forget about creating indexes — NoNoDB will do it for you if it decides it is
necessary.** Automatic indexing is opt-in (`NONODB_AUTO_INDEX=1`), conservative,
and removes unused automatic indexes after a configurable TTL.

These are capabilities often split across several services or reserved for
larger databases and paid enterprise editions. nonodb puts them in one small,
MIT-licensed binary: no paid tier, no license keys, no feature gates.

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
- no cross-collection joins (`$lookup`) or write stages (`$merge`/`$out`);
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
- Fast `find_one`/`find` paths that stop after enough matches, a raw JSON
  prefilter for simple string/boolean equalities, and `count({})`
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

### Aggregation

- Read-only `aggregate` endpoint for reporting, grouping, and summaries.
- Stages: `$match`, `$project`, `$addFields`/`$set`, `$unset`, `$unwind`,
  `$group`, `$sort`, `$skip`, `$limit`, and `$count`.
- Accumulators: `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`,
  `$addToSet`, and `$count`.
- A leading `$match` uses the normal query planner, including secondary indexes
  and `_id` ranges. `$sort`+`$limit`, `$skip`, `$limit`, simple `$project`, and
  a lone `$count` are also pushed into the query path where semantics allow.
- Expressions support field references (`"$field"`), `$$ROOT`, `$literal`,
  `$ifNull`, arithmetic, `$concat`, `$cond`, and comparison operators.

```bash
curl -s --unix-socket /tmp/nonodb.sock \
  -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"aggregate","coll":"orders","pipeline":[
        {"$match":{"status":"paid"}},
        {"$group":{"_id":"$tenant","total":{"$sum":"$amount"},"n":{"$sum":1}}},
        {"$sort":{"total":-1}},
        {"$limit":10}
      ]}'
```

Aggregation is intentionally bounded: an intermediate result may contain at
most 100,000 documents. It never writes to the database and is available inside
the read-only `batch` operation. See
[Aggregation](docs/api.md#aggregation) for the full stage list and semantics,
and [benchmarks](docs/benchmarks.md#aggregation-pipeline) for stage timings and
optimization notes.

### Document history

- Opt-in append-only revision history for inserts, updates, and deletes.
- Global monotonic revision IDs; no per-document counter lookup on writes.
- Current documents and indexes are unaffected: search always returns current
  results.
- Configurable retention with `NONODB_HISTORY_MAX_VERSIONS` (default `50`).
- Revisions are written in the same LMDB transaction as the current document.
- Read revisions with the `history` and `get_version` API operations, or the
  `history`/`get_version` helpers in the official clients.

Enable it on the daemon:

```bash
NONODB_HISTORY=1 NONODB_HISTORY_MAX_VERSIONS=50   NONODB_DATA=./data NONODB_SOCK=/tmp/nonodb.sock ./nonodb-daemon
```

Then inspect a document:

```bash
curl -s --unix-socket /tmp/nonodb.sock \
  -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"history","coll":"users","id":"<id>","include_docs":true}'

curl -s --unix-socket /tmp/nonodb.sock \
  -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"get_version","coll":"users","id":"<id>","version":1042}'
```

See [Document history](docs/api.md#document-history) for the full request and
retention semantics.

### Document TTL

- Optional collection default (`ttl_seconds`) and per-document `_ttl` override.
- Expiry metadata is stored outside the document; `_ttl` is not returned by
  reads.
- Updates preserve the current TTL unless `_ttl` is explicitly set.
- Background maintenance sweeps due documents through the single writer.
- TTL deletes remove index entries and append a history tombstone when history
  is enabled; collections can opt into purging stored revisions instead.
- Works for standard and vault collections.

```bash
curl -s --unix-socket /tmp/nonodb.sock \
  -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"create_collection","coll":"sessions","ttl_seconds":86400}'

curl -s --unix-socket /tmp/nonodb.sock \
  -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"insert","coll":"sessions","doc":{"_id":"s1","user":7,"_ttl":300}}'
```

TTL is eventual: a document can be visible briefly after its expiry. See
[Document TTL](docs/ttl.md) for update semantics, maintenance, and limits.

### Vault collections (optional encryption)

- Opt-in `profile: "vault"` collection; standard collections are unchanged.
- Whole-document XChaCha20-Poly1305 encryption; only `_id` and `tenant_id` remain
  plaintext.
- Per-request tenant key in headers: `X-NonoDB-Tenant`,
  `X-NonoDB-Key-Id`, `X-NonoDB-Key`.
- AAD binds ciphertext to collection, tenant, `_id`, and key id.
- Restricted API: CRUD by `_id`, list/count by `tenant_id`, history; no search,
  fuzzy, aggregation, secondary indexes, arbitrary filters, or sorting.
- History stores ciphertext and decrypts snapshots only for an authenticated
  key.
- Cache and automatic indexes are disabled for vault collections.

Create and use a vault collection:

```bash
KEY=$(python3 -c 'import base64, os; print(base64.b64encode(os.urandom(32)).decode())')

curl -s --unix-socket /tmp/nonodb.sock \
  -H 'content-type: application/json' \
  -d '{"op":"create_collection","coll":"vault","profile":"vault"}' \
  http://localhost/v1/exec

curl -s --unix-socket /tmp/nonodb.sock \
  -H 'content-type: application/json' \
  -H 'X-NonoDB-Tenant: acme' \
  -H 'X-NonoDB-Key-Id: acme:v1' \
  -H "X-NonoDB-Key: $KEY" \
  -d '{"op":"insert","coll":"vault","doc":{"tenant_id":"acme","notes":"secret"}}' \
  http://localhost/v1/exec
```

This is daemon-side encryption, not end-to-end encryption: the daemon sees
plaintext and the raw key in memory. Use it for encryption at rest when you can
accept the loss of query features. See
[Vault collections](docs/vault.md) for the full contract, limitations, key
rotation, and best-fit guidance.

### Operations

- Single daemon; Unix socket by default, optional TCP.
- Optional bearer-token authentication and Unix peer uid/gid allow-lists.
- Optional TLS for TCP using rustls, including mutual TLS with client
  certificate verification and a fingerprint allow-list for immediate
  revocation.
- Periodic LMDB snapshots with retention.
- Online backup with compaction; CLI restore and inspection.
- JSON metrics endpoint and request/queue/commit counters.
- Group commit with adaptive batching, request size limits, and request
  timeouts.
- Official thin clients for Python, Rust, Node.js, and Go.
- Docker image, systemd unit, crash tests, and end-to-end test scripts.

## Quick start

### Prebuilt binaries

Every tagged release contains ready-to-run binaries:

| Platform | Release asset |
| --- | --- |
| Linux x86_64 | `nonodb-vX.Y.Z-linux-x86_64.tar.gz` |
| macOS Apple Silicon | `nonodb-vX.Y.Z-macos-arm64.tar.gz` |
| macOS Intel | `nonodb-vX.Y.Z-macos-x86_64.tar.gz` |
| Windows x86_64 | `nonodb-vX.Y.Z-windows-x86_64.zip` |

Each archive contains `nonodb-daemon`, `nonodb-cli`, `README.md`, `LICENSE` and
`docs/`. Every archive also has a `.sha256` checksum on the
[releases page](https://github.com/essmos/nonodb/releases).

Linux and macOS use the Unix socket by default. Windows has no Unix socket
listener, so the Windows daemon requires TCP plus `NONODB_AUTH_TOKEN`.

### Run on Linux (native)

Requirements:

- Rust stable (2021 edition; latest stable recommended), Cargo, and a C
  compiler for LMDB.
- Linux or macOS for the Unix-socket daemon. The Windows daemon is TCP-only.
- Python 3.9+ only if you want the optional Python client (`httpx`; `orjson`
  is an optional speedup).

Build the release binaries:

```bash
cargo build --release
```

The binaries are:

```text
target/release/nonodb-daemon
target/release/nonodb-cli
```

Create the data and socket directories and start the daemon in the foreground:

```bash
mkdir -p /tmp/nonodb-data /tmp/nonodb-run

NONODB_DATA=/tmp/nonodb-data \
NONODB_SOCK=/tmp/nonodb-run/nonodb.sock \
RUST_LOG=info \
./target/release/nonodb-daemon
```

Leave that terminal running. In a second terminal, verify the daemon:

```bash
export NONODB_SOCK=/tmp/nonodb-run/nonodb.sock
./target/release/nonodb-cli ping
```

Insert and query with `curl`:

```bash
curl -s --unix-socket "$NONODB_SOCK" -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"insert","coll":"users","doc":{"name":"Ada","age":36}}'

curl -s --unix-socket "$NONODB_SOCK" -H 'content-type: application/json' \
  -X POST http://localhost/v1/exec \
  -d '{"op":"find","coll":"users","filter":{"age":{"$gte":18}},"sort":[["age",-1]]}'
```

Use the Python client:

```bash
export PYTHONPATH="$PWD/client"
python3 - <<'PY'
from nonodb import NonoDB

with NonoDB("/tmp/nonodb-run/nonodb.sock") as db:
    users = db.collection("users")
    users.insert({"name": "Grace", "age": 41})
    print(users.find({"age": {"$gte": 18}}, sort=[("age", -1)]))
PY
```

Use the CLI:

```bash
NONODB_SOCK=/tmp/nonodb-run/nonodb.sock ./target/release/nonodb-cli count users '{}'
NONODB_SOCK=/tmp/nonodb-run/nonodb.sock ./target/release/nonodb-cli find users '{}' --sort age:desc --limit 5
```

This runs in the foreground. For a production Linux service, use the systemd
unit and installation steps in [Operations](docs/operations.md#deployment-with-systemd).

### Run on macOS (native)

Download the archive matching your CPU (`uname -m` prints `arm64` or `x86_64`):

```bash
TAG=v0.5.0
curl -LO "https://github.com/essmos/nonodb/releases/download/$TAG/nonodb-$TAG-macos-arm64.tar.gz"
tar -xzf "nonodb-$TAG-macos-arm64.tar.gz"
cd "nonodb-$TAG-macos-arm64"
```

Release binaries are not signed or notarized. If Gatekeeper blocks them, remove
the quarantine attribute once:

```bash
xattr -d com.apple.quarantine nonodb-daemon nonodb-cli 2>/dev/null || true
```

Start the daemon exactly as on Linux (Unix socket and `SO_PEERCRED` work on
macOS):

```bash
mkdir -p /tmp/nonodb-data /tmp/nonodb-run

NONODB_DATA=/tmp/nonodb-data \
NONODB_SOCK=/tmp/nonodb-run/nonodb.sock \
RUST_LOG=info \
./nonodb-daemon
```

Then use `nonodb-cli`, `curl --unix-socket`, or the Python client with
`NONODB_SOCK=/tmp/nonodb-run/nonodb.sock`.

### Run on Windows (TCP only)

Download and extract `nonodb-vX.Y.Z-windows-x86_64.zip`, then start the daemon
from PowerShell. TCP is mandatory on Windows and always requires a bearer
token:

```powershell
$env:NONODB_DATA = "$PWD\data"
$env:NONODB_TCP = "127.0.0.1:7878"
$env:NONODB_AUTH_TOKEN = "replace-with-a-long-random-token"
$env:RUST_LOG = "info"
.\nonodb-daemon.exe
```

In a second PowerShell window:

```powershell
$env:NONODB_URL = "http://127.0.0.1:7878"
$env:NONODB_TOKEN = "replace-with-a-long-random-token"
.\nonodb-cli.exe ping
```

The Windows CLI uses plaintext HTTP over TCP. For TLS or mTLS connections, use
one of the official clients (`NONODB_URL=https://...`, `NONODB_TLS_CA`,
`NONODB_TLS_CLIENT_CERT`, `NONODB_TLS_CLIENT_KEY`). Keep the listener on
`127.0.0.1` unless you terminate TLS in front of it.

### Run with Docker

A published Docker image is not currently provided; build it from the
repository:

```bash
git clone https://github.com/essmos/nonodb.git
cd nonodb
docker build -t nonodb:local .
```

The simplest way to use it from a process on the host is TCP on loopback with a
bearer token:

```bash
docker volume create nonodb-data

docker run -d --name nonodb \
  -e NONODB_AUTH_TOKEN='replace-with-a-long-random-token' \
  -e NONODB_TCP=0.0.0.0:7878 \
  -p 127.0.0.1:7878:7878 \
  -v nonodb-data:/var/lib/nonodb \
  nonodb:local
```

Check health and logs:

```bash
curl -s http://127.0.0.1:7878/healthz
docker logs nonodb
```

Connect from Python:

```python
from nonodb import NonoDB

db = NonoDB(
    base_url="http://127.0.0.1:7878",
    token="replace-with-a-long-random-token",
)
print(db.collection("users").count())
db.close()
```

The named volume `nonodb-data` persists `/var/lib/nonodb`. The container runs as
the unprivileged `nonodb` user.

For container-to-container access over a Unix socket, mount a shared volume at
`/run/nonodb`, for example `-v nonodb-run:/run/nonodb`. The client container must
be able to access the socket with a matching uid/gid. For host processes, TCP on
`127.0.0.1` is usually simpler; add TLS if the listener is not loopback-only.

Docker Compose example:

```bash
export NONODB_AUTH_TOKEN='replace-with-a-long-random-token'
docker compose -f deploy/docker-compose.yml up -d
```

More deployment details: [Operations — Docker](docs/operations.md#docker).

## Documentation

| Document | What it covers |
|---|---|
| [Architecture](docs/architecture.md) | Daemon model, LMDB layout, write path, indexes, search, concurrency, crash safety |
| [HTTP API](docs/api.md) | Every operation, request/response shapes, filters, updates, indexes, search, errors |
| [Clients](docs/clients.md) | Python, Rust, Node.js and Go clients: install, CRUD, batches, indexes, search, TCP/TLS |
| [Mutual TLS](docs/mtls.md) | Client certificate setup, private CA, fingerprint allow-list, revocation, rotation |
| [Python client](docs/python-client.md) | `NonoDB`/`Collection` API, caching, retries, TCP/TLS, FastAPI usage |
| [Vault collections](docs/vault.md) | Optional encrypted tenant data, headers, restrictions, key rotation |
| [Document TTL](docs/ttl.md) | Collection defaults, per-document expiry, update semantics, maintenance |
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

## Sponsors

<p>
  <a href="https://liveapisec.com">
    <img src="docs/assets/liveapisec-logo.svg" width="44" height="44" alt="LiveAPISec">
  </a>
  &nbsp;
  <strong><a href="https://liveapisec.com">LiveAPISec</a></strong> supports
  nonodb development.
</p>

LiveAPISec provides continuous API pentesting with live, checkable scan
results. Visit [liveapisec.com](https://liveapisec.com) to learn more.

## Status

nonodb is an early-stage project (`0.5.0`). Its core storage, query, index,
search, batching, security, snapshot, and operational paths are covered by unit,
property-based, end-to-end, concurrency, crash, and large-data tests. The API
may still change before a stable release.

## License

MIT. See [LICENSE](LICENSE).
