# nonodb documentation

Start with the [project README](../README.md) for the problem nonodb solves,
quick-start instructions, and a feature overview.

## Guides

| Guide | Audience | Contents |
|---|---|---|
| [Architecture](architecture.md) | Maintainers, advanced users | Process model, LMDB layout, write path, indexes, search, aggregation, crash safety |
| [Multiple databases](multi-database.md) | Operators, application developers | Separate LMDB environments, URL routing, create/drop, CLI, GUI, migration, backup |
| [Adaptive writer](adaptive-writer.md) | Operators, performance reviewers | Adaptive group-commit window, latency/throughput tuning, metrics and limits |
| [GUI console](gui.md) | Operators, application developers | Embedded local React console, data editing, collection settings, SQL, map/metrics |
| [HTTP API](api.md) | All client authors | Operations, request/response formats, filters, updates, aggregation, errors |
| [SQL queries](sql.md) | Analysts, reporting jobs, application developers | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, filters, aggregates, parameters, limits, client examples |
| [Vault collections](vault.md) | Application developers, security reviewers | Optional whole-document encryption, tenant keys, restrictions, operations, key rotation |
| [Document TTL](ttl.md) | Application developers, operators | Collection defaults, per-document overrides, update semantics, background expiry, limits |
| [Dates and time zones](dates.md) | Application developers | BSON-compatible dates, RFC 3339 input, filtering, indexes, sorting, timezone semantics |
| [Numbers and decimals](numerics.md) | Application developers | Exact numeric comparison, Decimal128, arithmetic, NaN/Infinity, index compatibility, long text limits |
| [BSON scalar types](bson-types.md) | Application developers | `$numberInt`/`$numberLong`, `$type`, binary/UUID, ObjectId, index compatibility |
| [Clients](clients.md) | Application developers | Python, Rust, Node.js and Go: install, connection, CRUD, batches, indexes, search, aggregation |
| [Migration and import](migration.md) | Application developers, operators | MongoDB, PostgreSQL, SQLite, CSV conversion; NDJSON format; import manifest; validation and cutover |
| [Authentication and permissions](auth.md) | Operators, security reviewers | Principals, scopes, hashed tokens, certificate registry, lockout, reloads, audit log |
| [Security checks](security-check.md) | Operators, security reviewers, maintainers | Local security regression checks, LiveAPISec audit method, results, hardening gaps, production checklist |
| [Monitoring and alerting](monitoring.md) | Operators, SRE | Health/readiness, Prometheus endpoint, metrics:read token, OpenTelemetry Collector, disk/snapshot metrics, alerts |
| [Mutual TLS](mtls.md) | Operators, security reviewers | Private CA setup, client certificates, fingerprint mapping, revocation, rotation, troubleshooting |
| [Python client](python-client.md) | Python developers | Connection, CRUD, upsert, indexes, search, batches, cache, FastAPI |
| [Background jobs](workers.md) | Application developers, operators | Durable jobs, workers, leases, retries, cron/systemd, languages |
| [Outbox events](events.md) | Application developers, operators | Atomic data + events, automatic collection events, source-side filters, polling consumers, checkpoints, event worker CLI, TTL, systemd |
| [Conditional subscriptions](subscriptions.md) | Application developers | Conditions over collection state, count thresholds, edge-triggered mode, events, clients |
| [Streaming outbox events](streams.md) | Application developers | Long-poll `watch`, NDJSON/SSE streams, resume tokens, filters, truncation, Python/Rust/Node.js/Go examples |
| [CLI](cli.md) | Operators | Administration, map resize, backup verification, restore drills, inspect, raw requests |
| [Operations](operations.md) | Operators, SRE | Configuration, deployment, security, snapshots, monitoring, capacity, troubleshooting |
| [Backup and restore](backup.md) | Operators, SRE | Online backup, GUI backup/import, snapshots, verification, restore drills, Docker, vault keys, authorization state, recovery |
| [Benchmarks](benchmarks.md) | Evaluators, maintainers | Environment, methodology, throughput, latency, memory, large-data results |
| [Development](development.md) | Contributors | Repository layout, build, tests, scripts, conventions |
| [Roadmap and limits](roadmap.md) | Everyone | Supported features, deliberate limitations, future work |

## Common tasks

- **Download prebuilt binaries:** [README — Prebuilt binaries](../README.md#prebuilt-binaries)
- **Run on Linux, macOS or Windows:** [README — Quick start](../README.md#quick-start)
- **Run with Docker:** [README — Run with Docker](../README.md#run-with-docker)
- **Run several databases in one daemon:** [Multiple databases](multi-database.md)
- **Open the local GUI:** [GUI console](gui.md)
- **Pick a client:** [Clients](clients.md) — Python, Rust, Node.js, Go
- **Run a SQL query or simple write:** [SQL queries](sql.md)
- **Import data from another database:** [Migration and import](migration.md)
- **Expose nonodb to Python workers:** [Python client](python-client.md)
- **Run background jobs and schedulers:** [Background jobs](workers.md)
- **Consume change events:** [Outbox events](events.md)
- **React to collection conditions:** [Conditional subscriptions](subscriptions.md)
- **Stream events with a resume token:** [Streaming outbox events](streams.md)
- **Secure a deployment:** [Authentication and authorization](auth.md) and
  [TCP/TLS](operations.md#tcp-and-tls)
- **Run security checks:** [Security checks](security-check.md)
- **Tune writer latency:** [Adaptive writer](adaptive-writer.md)
- **Set up client certificates:** [Mutual TLS](mtls.md)
- **Store sensitive tenant data encrypted:** [Vault collections](vault.md)
- **Expire documents automatically:** [Document TTL](ttl.md)
- **Filter and index timestamps:** [Dates and time zones](dates.md)
- **Store money and exact decimals:** [Numbers and decimals](numerics.md)
- **Use binary, UUID, ObjectId, or `$type`:** [BSON scalar types](bson-types.md)
- **Back up and restore:** [Backup and restore](backup.md)
- **Import a backup as a new database:** [Import wizard](backup.md#import-wizard)
- **Tune timeouts for index builds:** [Timeouts](operations.md#timeouts-and-long-operations)
- **Understand memory usage:** [Capacity and memory](operations.md#capacity-and-memory)
- **Reproduce performance numbers:** [Benchmarks](benchmarks.md#reproducing)
