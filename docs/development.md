# Development

This document describes the repository layout, build and test workflow, and the
benchmark/test scripts. For runtime behavior, see [Architecture](architecture.md)
and [HTTP API](api.md).

## Repository layout

```text
.
├── Cargo.toml                 # workspace
├── nonodb-auth/               # auth-file schema, principals, scopes, token hashing
├── nonodb-core/               # storage, collections, indexes, query, search
│   ├── src/
│   │   ├── lib.rs
│   │   ├── store.rs           # LMDB environment and collection registry
│   │   ├── collection.rs      # CRUD, query planning, aggregate pushdown, scans
│   │   ├── aggregate.rs       # read-only aggregation stages and expressions
│   │   ├── index.rs           # order-preserving index encoding
│   │   ├── search.rs          # tokenization, BM25, trigrams, Levenshtein
│   │   ├── vault.rs           # optional encrypted collections
│   │   ├── ttl.rs             # expiry metadata and due-entry scan
│   │   ├── date.rs            # BSON-compatible Extended JSON dates
│   │   ├── numeric.rs         # exact numeric/Decimal128 comparison and indexing
│   │   ├── bson_types.rs      # binary/UUID, ObjectId, $type helpers
│   │   ├── regex.rs           # safe $regex compilation and index-prefix hints
│   │   ├── error.rs
│   │   └── query/
│   │       ├── matcher.rs     # MongoDB-like filters
│   │       ├── validate.rs    # regex and array-operator argument validation
│   │       ├── updater.rs     # update operators and upsert seeding
│   │       └── path.rs        # dotted paths
│   ├── tests/
│   │   ├── properties.rs      # proptest
│   │   ├── reopen.rs          # persistence/reopen
│   │   ├── ttl.rs             # TTL defaults, overrides, sweeper, vault
│   │   ├── dates.rs           # date parsing, filtering, indexes, sorting
│   │   ├── numerics.rs        # exact numbers, Decimal128, arithmetic, indexes
│   │   ├── bson_types.rs      # numberInt/Long, $type, binary/UUID, ObjectId
│   │   ├── progress.rs        # process-local index-backfill progress
│   │   ├── regex.rs           # $regex/$options, arrays, invalid patterns
│   │   ├── arrays.rs          # $all, $size, $elemMatch, validation
│   │   └── vault.rs           # vault encryption and tenant isolation
│   └── examples/
│       ├── backup.rs
│       └── inspect.rs
├── nonodb-daemon/             # HTTP server, writer thread, metrics
│   └── src/
│       ├── main.rs            # listeners, TLS handshake, snapshots
│       ├── auth.rs            # connection identity, token/cert auth, file reload
│       ├── authz.rs           # request -> required scopes
│       ├── login_guard.rs     # per-IP failed-login sliding window and lockout
│       ├── audit.rs           # JSONL audit log
│       ├── protocol.rs        # request/response types
│       ├── writer.rs          # single writer + group commit
│       ├── maintenance.rs     # TTL sweeps, history pruning, auto-index drive
│       ├── auto_index.rs      # automatic index candidates/state
│       ├── cache.rs           # read cache
│       ├── cursor.rs          # keyset cursor registry and ownership
│       └── metrics.rs
├── nonodb-cli/                # administrative CLI
├── nonodb-client/             # Rust client crate and example
├── client/                    # Python client and tests
│   ├── node/                  # zero-dependency Node.js client
│   └── go/                    # context-aware Go client
├── workers/python/            # external background-job helper and examples
├── scripts/                   # benchmarks, concurrency/crash/search tests
├── deploy/                    # systemd units
├── docs/                      # documentation
├── Dockerfile
└── README.md
```

## Prerequisites

- Rust stable with Cargo and a C compiler for LMDB.
- Linux or macOS for the Unix-socket daemon. Windows builds are TCP-only.
- Python 3.9+ and `httpx` for client tests. `orjson` is optional.
- Node.js 22+ for the GUI and Node client; Go 1.22+ for the Go client.
- For large benchmarks: enough disk space and a real filesystem (`ext4`), not
  tmpfs.

## Build

Rebuild the embedded GUI assets before compiling the CLI:

```bash
(cd gui && npm ci && npm run build)
cargo build --release
cargo build --release --examples
```

CI and Docker build the GUI automatically before Cargo. A fresh checkout contains
`gui/dist/.gitkeep`, so Cargo can compile even before the frontend build runs.

Binaries:

```text
target/release/nonodb-daemon
target/release/nonodb-cli
```

## Run locally

```bash
mkdir -p ./.dev-data
NONODB_DATA=./.dev-data \
NONODB_SOCK=/tmp/nonodb-dev.sock \
RUST_LOG=info \
./target/release/nonodb-daemon
```

In another shell:

```bash
S=/tmp/nonodb-dev.sock
NONODB_SOCK=$S ./target/release/nonodb-cli ping
NONODB_SOCK=$S python3 client/smoke_test.py
```

Use `kill -TERM` or Ctrl-C to stop the daemon. It handles both signals.

## Formatting and linting

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

CI should run both. The project treats Clippy warnings as errors.

## Tests

```bash
cargo test
```

This runs:

- unit tests in `nonodb-core` for filters, updates, paths, index encoding,
  unique constraints, query planning, Top-K, limits, BM25, fuzzy search,
  numbers, decimals, binary/ObjectId, `$type`, and JSON edge cases;
- integration tests for dates, exact numerics/Decimal128, BSON scalar types,
  arrays and array updates, find-and-modify, explain, sub-database slots, online
  map resize, backup verification, TTL, vault, and persistence/reopen;
- property-based tests with `proptest` for encoding and basic query semantics;
- auth-crate tests for schema validation, token hashing, certificate lookup, and
  scope matching;
- daemon tests for authorization mapping, cache, automatic-index selection, and
  shutdown drain helpers;
- Rust client unit/doc tests.

The latest run: **5 auth-crate tests, 135 core unit tests, 23 daemon tests, 5
property tests, 15 date tests, 11 numeric tests, 8 BSON scalar-type tests, 5
regex tests, 5 array query tests, 5 array-update tests, 5 find-and-modify tests,
4 explain tests, 3 sub-database slot tests, 5 online-resize tests, 6
backup-verification tests, 9 TTL tests, 3 vault tests, 20 SQL tests, 2 SQL parser fuzz tests, 1 reopen
test, and 2 Rust client unit tests**, all passing (295 total including
doc-tests).

### End-to-end scripts

Start a daemon on a test database first, then run:

```bash
export NONODB_SOCK=/tmp/nonodb.sock

python3 -m unittest discover -s workers/python/tests -v  # job helper units
python3 client/smoke_test.py                 # CRUD, concurrent $inc, errors
python3 client/cache_test.py                 # TTL, invalidation, copies
python3 scripts/batch_test.py                # read batch + get_many + cache
python3 scripts/write_batch_test.py          # atomic commit/rollback
python3 scripts/upsert_test.py               # upsert and array operators
python3 scripts/dates_test.py                # Extended JSON dates, ranges, indexes
python3 scripts/numerics_test.py             # exact numbers, decimal arithmetic/indexes
python3 scripts/bson_types_test.py           # numberInt/Long, $type, binary/UUID, ObjectId
python3 scripts/regex_test.py                # $regex/$options, arrays, errors
python3 scripts/arrays_test.py               # $all, $size, $elemMatch, validation
python3 scripts/array_updates_test.py        # positional/array-filter updates
python3 scripts/find_and_modify_test.py      # atomic select-update-return
python3 scripts/explain_test.py              # plans, bounds, actual counts
python3 scripts/sql_test.py                  # SQL SELECT/INSERT/UPDATE/distinct/having/explain/params/batch (spawns a daemon)
python3 scripts/sql_matrix_test.py           # exhaustive SQL matrix, concurrency, restart, metrics, CLI (spawns a daemon)
PYTHONPATH=client:workers/python python3 workers/python/examples/jobs_smoke.py  # jobs
PYTHONPATH=client:workers/python python3 workers/python/examples/events_smoke.py  # events
python3 workers/python/tests/events_integration.py  # events against a spawned daemon
python3 scripts/db_slots_test.py             # >256 index/search create-drop cycles
python3 scripts/resize_test.py               # online map growth under concurrent I/O
python3 scripts/restore_drill.py             # backup -> verify -> restore -> query
python3 scripts/shutdown_test.py             # SIGTERM drain, JSON logs, durable in-flight write
python3 scripts/auth_test.py                 # scopes, token/cert auth, reload, audit, IP lockout
python3 scripts/ttl_test.py                  # TTL sweeps, overrides, history, vault
python3 scripts/history_test.py              # per-collection history enable/disable, restart, CLI
python3 scripts/events_auto_test.py          # automatic collection events, field filters, thresholds, CLI
python3 scripts/subscriptions_test.py        # persistent conditional subscriptions, count, once, SQL, restart
python3 scripts/live_watch_test.py           # connection-scoped live mode, direct SSE delivery, no _events
python3 scripts/gui_test.py                  # embedded GUI assets and local API proxy
python3 scripts/multi_database_test.py       # routing, isolation, cursors, restart, drop
python3 scripts/multi_database_features_test.py  # full feature matrix inside app1
python3 scripts/durability_test.py             # full/relaxed/unsafe, sync, crash/restart
python3 scripts/vault_test.py                # optional vault encryption end-to-end
python3 scripts/vault_regression_test.py     # vault API regressions
python3 scripts/vault_bench.py --docs 5000   # standard vs vault performance
bash scripts/mtls_test.sh                    # mTLS, cert registry, live revocation
python3 scripts/search_test.py               # BM25 + fuzzy end-to-end
python3 scripts/search_variants_test.py      # search/query edge cases
python3 scripts/compound_test.py             # compound/covering indexes, phrases
python3 scripts/concurrency_test.py          # 12 processes, unique index races
```

Client smoke tests can also be run together, including starting a disposable
daemon:

```bash
bash scripts/clients_test.sh
```

This runs the Rust, Node.js, and Go clients against the same daemon.

Some scripts expect to create their own collections and may leave data behind;
use a disposable database.

### Crash tests

```bash
BIN=./target/release/nonodb-daemon bash scripts/crash_test.sh
```

The script starts a daemon, writes concurrently, kills it with `SIGKILL`,
restarts it, and verifies committed data and successful writes afterward.

For a larger run, use `scripts/large_crash_test.sh` against a 1M+ database. It
expects `NONODB_DATA`, `NONODB_SOCK`, and `NONODB_PID` for the running daemon.

### Preforking web server test

```bash
BIN=./target/release/nonodb-daemon bash scripts/uvicorn_test.sh
```

This starts a daemon and a uvicorn app with 12 workers, then issues concurrent
`$inc` requests and verifies the final counter.

### Docker

```bash
docker build -t nonodb:local .
docker run --rm -d --name nonodb-test \
  -v nonodb-test-data:/var/lib/nonodb \
  -v nonodb-test-run:/run/nonodb \
  nonodb:local
```

Use a named volume for `/run/nonodb`, not a single-file bind mount. The socket is
recreated on each start.

## Benchmarks and large-data tests

See [Benchmarks](benchmarks.md) for the full methodology and results. The main
scripts are:

| Script | Purpose |
|---|---|
| `scripts/bench.py` | Mixed CRUD throughput/latency with multiple processes |
| `scripts/opt_bench.py` | Count, early exit, index update, batch comparison |
| `scripts/aggregate_bench.py` | Per-stage aggregation timings and report pipelines |
| `scripts/sql_test.py` | End-to-end SQL against a disposable daemon |
| `scripts/dates_test.py` | Date offsets, ranges, indexes, sorting, unique constraints |
| `scripts/numerics_test.py` | Large integers, Decimal128, special doubles, arithmetic, indexes |
| `scripts/bson_types_test.py` | `$numberInt`/`$numberLong`, `$type`, binary/UUID, ObjectId |
| `scripts/regex_test.py` | `$regex`/`$options`, arrays, invalid patterns, index prefixes |
| `scripts/arrays_test.py` | `$all`, `$size`, `$elemMatch`, validation, index/aggregation/search |
| `scripts/array_updates_test.py` | positional `$`, `$[]`, `arrayFilters`, push modifiers, `$pull` |
| `scripts/find_and_modify_test.py` | atomic old/new return, sort, upsert, remove, concurrent counters |
| `scripts/explain_test.py` | access paths, index bounds, sort coverage, actual counts |
| `scripts/db_slots_test.py` | index/text/fuzzy/vector slot reuse above the legacy 256 limit |
| `scripts/resize_test.py` | online map growth, concurrent I/O, rejected shrink |
| `scripts/restore_drill.py` | live backup, manifest verification, restore, query checks |
| `scripts/shutdown_test.py` | graceful shutdown with an in-flight atomic write, JSON logs, restart durability |
| `scripts/auth_test.py` | principals, scopes, filtered collections, token revocation, audit, IP lockout |
| `scripts/ttl_test.py` | TTL defaults, overrides, update semantics, sweeper, vault |
| `scripts/vault_test.py` | Vault encryption, tenant isolation, and restricted API |
| `scripts/vault_regression_test.py` | Vault profiles, headers, filters, CRUD, errors, history |
| `scripts/history_test.py` | Per-collection history retention, isolation, restart durability, CLI |
| `scripts/events_auto_test.py` | Automatic collection events, source-side filters/projections, find_and_modify/TTL/SQL writes, watch filters, thresholds, metrics, CLI tail |
| `scripts/vault_bench.py` | Standard vs vault insert/read/update/count performance |
| `scripts/cpu_bench.py` | Samples daemon CPU from `/proc` |
| `scripts/large_test.py` | 1M+ document load, indexes, query latency, RAM |
| `scripts/large_crash_test.sh` | Crash during writes on a large database |
| `scripts/uvicorn_test.sh` | 12 uvicorn workers against one daemon |
| `scripts/clients_test.sh` | Rust, Node.js, Go client smoke tests, dates, TTL, vault, auth, shutdown, mTLS |
| `scripts/mtls_test.sh` | mTLS with generated CA/certs, all clients, live revocation |
| `scripts/concurrency_test.py` | Independent writers/readers and unique indexes |

Put benchmark data on the target filesystem. If `/tmp` is tmpfs, write
benchmarks there will overstate durability performance.

For 2M+ documents, raise `NONODB_MAP_SIZE` (for example `4GiB` or `8GiB`) and
increase request timeouts for index backfills.

## Code conventions

- Keep storage logic in `nonodb-core`; the daemon should not implement query
  semantics.
- Keep the HTTP protocol in `nonodb-daemon/src/protocol.rs`.
- Validate user input in core methods and return `nonodb_core::Error`; the
  daemon maps errors to API codes.
- Preserve backward compatibility for public core methods where practical. New
  result shapes can be added through new methods (for example
  `update_result`/`update_opts_txn`).
- Keep index maintenance in the same transaction as document writes.
- Add a unit test for matcher/updater/index changes and an end-to-end script
  when the HTTP surface changes.
- Run `cargo fmt` and `cargo clippy --all-targets -- -D warnings` before
  submitting changes.
- Do not commit generated data directories, LMDB files, or benchmark output.

## Adding a query operator or update operator

1. Implement matching in `nonodb-core/src/query/matcher.rs` or updating in
   `updater.rs`.
2. Add unit tests, including missing fields, `null`, type mismatches, and nested
   paths.
3. If the operator can use an index, update the query planner conservatively.
   Never use an index for a condition it cannot prove equivalent; fall back to a
   full scan and residual matching.
4. If the operator affects index maintenance, update
   `apply_index_entries` and the text/fuzzy/vector maintenance paths, and add a
   compound test.
5. Document it in [HTTP API](api.md) and the Python client if exposed.

## Adding an API operation

1. Add a variant to `Request` (read) or `WriteOp`/`WriteReq` (write) in
   `protocol.rs`.
2. Implement the core method with a transaction-aware `*_txn` or `*_in`
   variant.
3. Wire the operation in `main.rs` (read path) or `writer.rs` (write path).
4. Add request/response documentation.
5. Add a client wrapper if appropriate.
6. Add an end-to-end test and update `README.md`/`docs/api.md`.

## Continuous integration

GitHub Actions runs on every push to `main`, every pull request, and manually:

```text
.github/workflows/ci.yml
```

The workflow installs stable Rust with `rustfmt` and `clippy`, restores the
Cargo cache, then runs:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
cargo build --locked --release --bins --examples
python3 -m compileall -q client scripts
```

A pull request should be green before merging.

## Releasing

Releases are built and published by:

```text
.github/workflows/release.yml
```

There are two supported flows.

### Release from a tag

GitHub Actions builds every release artifact. Do not build or upload release
binaries locally.

Use one title format everywhere:

```text
vX.Y.Z - Short summary
```

The tag is `vX.Y.Z`; the annotated tag subject and GitHub Release title are
`vX.Y.Z - Short summary`:

```bash
# final release
git tag -a v0.21.0 -m "v0.21.0 - GUI login and authentication"
git push origin v0.21.0

# pre-release (the hyphen makes it a GitHub pre-release automatically)
git tag -a v0.21.0-beta.1 -m "v0.21.0-beta.1 - Embedded GUI console"
git push origin v0.21.0-beta.1
```

The release workflow reads the tag annotation and publishes the release with
the same `vX.Y.Z - Short summary` title. If the annotated tag body contains
release notes, the workflow uses that body as the GitHub Release description;
otherwise it falls back to GitHub-generated notes. Keep the subject as the
title and put the full notes in the body.

When the notes are stored in a file, preserve Markdown headings with:

```bash
git tag -a vX.Y.Z -F /tmp/nonodb-vX.Y.Z-tag.txt --cleanup=verbatim
```

The file should start with `vX.Y.Z - Short summary`, then a blank line, then the
notes. `--cleanup=verbatim` prevents Git from stripping lines that start with
`#`.

The workflow runs the tests, builds release binaries for Linux x86_64, macOS
Intel, macOS Apple Silicon, and Windows x86_64, attaches checksums, and
publishes the GitHub Release. Linux and macOS use `.tar.gz`; Windows uses
`.zip`. Tags such as `v0.21.0-beta.1` and `v0.21.0-rc.1` are marked as
pre-releases automatically.

### Manual release

Use **Actions → Release → Run workflow** and provide:

| Input | Meaning |
|---|---|
| `tag` | Tag to publish, e.g. `v0.21.0` or `v0.21.0-rc.1` |
| `summary` | Short title text after the version: `vX.Y.Z - <summary>` |
| `prerelease` | Mark the release as a pre-release |
| `draft` | Create it as a draft for review |

If `summary` is empty, the workflow uses the annotated tag subject. If neither
provides a summary, the title falls back to `vX.Y.Z - Release`. If the annotated
tag has a body, the workflow uses it as the release notes; otherwise it uses
GitHub-generated notes.

If the tag does not exist, `gh release create` creates it at the commit that was
checked out. For a new release, this is normally the current `main` commit.

### Release assets

Each release contains:

```text
nonodb-<tag>-linux-x86_64.tar.gz
nonodb-<tag>-linux-x86_64.tar.gz.sha256
```

The tarball contains `nonodb-daemon`, `nonodb-cli`, `README.md`, `LICENSE`, and
the `docs/` directory.

### Re-running a release

If the GitHub Release already exists, the workflow uploads the newly built
assets with `--clobber`. This allows a failed packaging or publishing step to be
retried without deleting the release manually.

## Pull request checklist

- [ ] `cargo fmt --check`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo test`
- [ ] Relevant end-to-end script passes
- [ ] New behavior is documented
- [ ] No data directories, secrets, or benchmark artifacts committed
