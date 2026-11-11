# Test results

The full benchmark and test report is maintained in
**[docs/benchmarks.md](docs/benchmarks.md)**. This page keeps the high-level
status and points to the detailed methodology and tables, so there is one source
of truth for performance numbers.

## Latest status

Measured on 2026-11-09 (UTC) on an 8 vCPU Haswell host, 22.9 GiB RAM, ext4,
release build.

| Check | Result |
|---|---|
| `cargo build --release --examples` | OK |
| `cargo fmt --check` | OK |
| `cargo clippy --all-targets -- -D warnings` | 0 warnings |
| `cargo test` | Core/daemon unit tests, 5 property-based, reopen, TTL, and vault integration tests, all passing |
| CRUD, cache, batch, write batch, upsert, search, compound indexes | OK |
| 12-process concurrency and unique-index race tests | OK |
| `kill -9` during writes | No corruption; committed data readable |
| 12 uvicorn workers against one daemon | OK |
| Live backup and restore, 1M and 8M documents | OK |
| Docker build and named-volume run | OK |
| Vault encryption regression suite | OK (profiles, headers, isolation, CRUD, history, rejected ops) |
| Auth registry, scopes, and lockout | OK (tokens, scopes, filtered list, live reload, audit, per-IP blocks) |
| mTLS with all official clients | OK (Python, Rust, Node.js, Go; anonymous and revoked certs rejected) |
| Document TTL suite | OK (defaults, overrides, update semantics, sweeper, history tombstones/purge, vault) |

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

Detailed tables, caveats, and reproduction commands:
[docs/benchmarks.md](docs/benchmarks.md).
