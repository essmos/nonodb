# Development

This document describes the repository layout, build and test workflow, and the
benchmark/test scripts. For runtime behavior, see [Architecture](architecture.md)
and [HTTP API](api.md).

## Repository layout

```text
.
├── Cargo.toml                 # workspace
├── nonodb-core/               # storage, collections, indexes, query, search
│   ├── src/
│   │   ├── lib.rs
│   │   ├── store.rs           # LMDB environment and collection registry
│   │   ├── collection.rs      # CRUD, query planning, aggregate pushdown, scans
│   │   ├── aggregate.rs       # read-only aggregation stages and expressions
│   │   ├── index.rs           # order-preserving index encoding
│   │   ├── search.rs          # tokenization, BM25, trigrams, Levenshtein
│   │   ├── error.rs
│   │   └── query/
│   │       ├── matcher.rs     # MongoDB-like filters
│   │       ├── updater.rs     # update operators and upsert seeding
│   │       └── path.rs        # dotted paths
│   ├── tests/
│   │   ├── properties.rs      # proptest
│   │   └── reopen.rs          # persistence/reopen
│   └── examples/
│       ├── backup.rs
│       └── inspect.rs
├── nonodb-daemon/             # HTTP server, writer thread, metrics
│   └── src/
│       ├── main.rs            # listeners, auth, TLS, snapshots
│       ├── protocol.rs        # request/response types
│       ├── writer.rs          # single writer + group commit
│       └── metrics.rs
├── nonodb-cli/                # administrative CLI
├── nonodb-client/             # Rust client crate and example
├── client/                    # Python client and tests
│   ├── node/                  # zero-dependency Node.js client
│   └── go/                    # context-aware Go client
├── scripts/                   # benchmarks, concurrency/crash/search tests
├── deploy/                    # systemd unit
├── docs/                      # documentation
├── Dockerfile
└── README.md
```

## Prerequisites

- Rust stable with Cargo and a C compiler for LMDB.
- Linux or macOS for the Unix-socket daemon. Windows builds are TCP-only.
- Python 3.9+ and `httpx` for client tests. `orjson` is optional.
- Node.js 18+ for the Node client; Go 1.22+ for the Go client.
- For large benchmarks: enough disk space and a real filesystem (`ext4`), not
  tmpfs.

## Build

```bash
cargo build --release
cargo build --release --examples
```

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
  unique constraints, query planning, Top-K, limits, BM25, fuzzy search, and
  JSON edge cases;
- property-based tests with `proptest` for encoding and basic query semantics;
- a persistence/reopen integration test;
- Rust client unit/doc tests.

The latest run: **65 core unit tests (including aggregation and pushdown),
7 daemon tests (cache and automatic-index selection), 5 property tests, 1
reopen test, and 2 Rust client unit tests**, all passing.

### End-to-end scripts

Start a daemon on a test database first, then run:

```bash
export NONODB_SOCK=/tmp/nonodb.sock

python3 client/smoke_test.py                 # CRUD, concurrent $inc, errors
python3 client/cache_test.py                 # TTL, invalidation, copies
python3 scripts/batch_test.py                # read batch + get_many + cache
python3 scripts/write_batch_test.py          # atomic commit/rollback
python3 scripts/upsert_test.py               # upsert and array operators
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
| `scripts/cpu_bench.py` | Samples daemon CPU from `/proc` |
| `scripts/large_test.py` | 1M+ document load, indexes, query latency, RAM |
| `scripts/large_crash_test.sh` | Crash during writes on a large database |
| `scripts/uvicorn_test.sh` | 12 uvicorn workers against one daemon |
| `scripts/clients_test.sh` | Rust, Node.js, Go client smoke tests |
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
   `apply_index_entries`/text/fuzzy maintenance and add a compound test.
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

Create and push an annotated tag:

```bash
# final release
git tag -a v0.3.0 -m "nonodb v0.3.0"
git push origin v0.3.0

# pre-release (the hyphen makes it a GitHub pre-release automatically)
git tag -a v0.4.0-beta.1 -m "nonodb v0.4.0-beta.1"
git push origin v0.4.0-beta.1
```

The workflow runs the tests, builds release binaries for Linux x86_64, macOS
Intel, macOS Apple Silicon, and Windows x86_64, attaches checksums, and publishes
a GitHub Release. Linux and macOS use `.tar.gz`; Windows uses `.zip`. Tags such
as `v0.4.0-beta.1` and `v0.4.0-rc.1` are marked as pre-releases automatically.

### Manual release

Use **Actions → Release → Run workflow** and provide:

| Input | Meaning |
|---|---|
| `tag` | Tag to publish, e.g. `v0.3.0` or `v0.4.0-rc.1` |
| `prerelease` | Mark the release as a pre-release |
| `draft` | Create it as a draft for review |

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
