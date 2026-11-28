# nonodb documentation

Start with the [project README](../README.md) for the problem nonodb solves,
quick-start instructions, and a feature overview.

## Guides

| Guide | Audience | Contents |
|---|---|---|
| [Getting started](getting-started.md) | New users | Docker Compose, prebuilt binary, build from source, first document, configuration basics |
| [Architecture](architecture.md) | Maintainers, advanced users | Process model, LMDB layout, write path, indexes, search, aggregation, crash safety |
| [Multiple databases](multi-database.md) | Operators, application developers | Separate LMDB environments, URL routing, create/drop, CLI, GUI, migration, backup |
| [Per-database quotas](quotas.md) | Operators, SRE | Data/index/history/sidecar accounting, soft warnings, hard write rejection, CLI |
| [Adaptive writer](adaptive-writer.md) | Operators, performance reviewers | Adaptive group-commit window, latency/throughput tuning, metrics and limits |
| [GUI console](gui.md) | Operators, application developers | Embedded local React console, data editing, collection settings, SQL, map/metrics |
| [HTTP API](api.md) | All client authors | Operations, request/response formats, filters, updates, aggregation, errors |
| [MongoDB compatibility](mongodb-compatibility.md) | Evaluators, migrating teams | Documented MongoDB-compatible subset, supported drivers, known differences, and migration assessment |
| [MongoDB wire shadow mode](mongodb-shadow.md) | Evaluators, migration engineers | Opt-in prototype dual-write to MongoDB, JSONL diff report, and limitations |
| [SQL queries](sql.md) | Analysts, reporting jobs, application developers | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, filters, aggregates, parameters, limits, client examples |
| [Vault collections](vault.md) | Application developers, security reviewers | Optional whole-document encryption, tenant keys, restrictions, operations, key rotation |
| [Document TTL](ttl.md) | Application developers, operators | Collection defaults, date-field MongoDB TTL, per-document overrides, strict read visibility, update semantics, limits |
| [Dates and time zones](dates.md) | Application developers | BSON-compatible dates, RFC 3339 input, filtering, indexes, sorting, timezone semantics |
| [Time-series collections](timeseries.md) | Application developers, evaluators | `profile: "timeseries"`, per-series/window buckets, columnar codecs, aggregation, retention, indexes, SQL, replication, limits |
| [Columnar collections](columnar.md) | Application developers, evaluators | `profile: "columnar"`, general-purpose buckets, settings, API parity, indexes, TTL, backup, migration, downgrade |
| [Time-series benchmark vs TimescaleDB](timescale-benchmark.md) | Evaluators, maintainers | Dataset, methodology, results, caveats, and reproduction for NonoDB and TimescaleDB |
| [Columnar collections plan](columnar-collections-plan.md) | Maintainers, evaluators | Proposed general-purpose bucket-columnar profile, architecture, phases, benchmark plan, and risks |
| [Columnar collections acceptance](columnar-collections-acceptance.md) | Maintainers, reviewers | Mandatory semantic, operational, and performance parity gates for the columnar profile |
| [Columnar Phase 0 report](columnar-phase-0-report.md) | Maintainers, reviewers | Feasibility benchmark, id-map/update/point-read measurements, and Phase 1 decisions |
| [Columnar performance evidence](columnar-performance.md) | Maintainers, reviewers | Canonical standard-vs-columnar release gates, retention/compaction, and pathological fallbacks |
| [Numbers and decimals](numerics.md) | Application developers | Exact numeric comparison, Decimal128, arithmetic, NaN/Infinity, index compatibility, long text limits |
| [BSON scalar types](bson-types.md) | Application developers | `$numberInt`/`$numberLong`, `$type`, binary/UUID, ObjectId, Timestamp, MinKey/MaxKey, index compatibility |
| [Clients](clients.md) | Application developers | Python, Rust, Node.js and Go: install, connection, CRUD, batches, indexes, search, aggregation |
| [Migration and import](migration.md) | Application developers, operators | MongoDB, PostgreSQL, SQLite, CSV conversion; NDJSON format; import manifest; validation and cutover |
| [Authentication and permissions](auth.md) | Operators, security reviewers | Principals, scopes, hashed tokens, certificate registry, lockout, reloads, audit log |
| [Security checks](security-check.md) | Operators, security reviewers, maintainers | Local security regression checks, LiveAPISec audit method, results, hardening gaps, production checklist |
| [Monitoring and alerting](monitoring.md) | Operators, SRE | Health/readiness, Prometheus endpoint, metrics:read token, OpenTelemetry Collector, disk/snapshot metrics, alerts |
| [Mutual TLS](mtls.md) | Operators, security reviewers | Private CA setup, client certificates, fingerprint mapping, revocation, rotation, troubleshooting |
| [Python client](python-client.md) | Python developers | Connection, CRUD, upsert, indexes, search, batches, cache, FastAPI, async watch/streams |
| [Background jobs](workers.md) | Application developers, operators | Durable jobs, workers, leases, retries, cron/systemd, languages |
| [Outbox events](events.md) | Application developers, operators | Atomic data + events, automatic collection events, source-side filters, polling consumers, checkpoints, event worker CLI, TTL, systemd |
| [Conditional subscriptions](subscriptions.md) | Application developers | Conditions over collection state, count thresholds, edge-triggered mode, events, clients |
| [Search and Atlas compatibility](search.md) | Application developers | Unified `search_all`, BM25, fuzzy, vector, hybrid RRF, and Atlas migration |
| [Graph traversal](graph.md) | Application developers | Nodes and edges as documents, adjacency index, BFS/DFS, shortest/all-shortest paths, pattern matching, PageRank/degree/components, filters, hybrid search, online rebuild, GUI tree, limits |
| [Graph benchmark](graph-benchmark.md) | Evaluators, maintainers | NonoDB vs Neo4j Community: dataset, methodology, results, caveats, reproduction |
| [Read-only Cypher](cypher.md) | Application developers, evaluators | Cypher API, supported clauses, GDS procedures, limits, pinned TCK baseline |
| [Streams and consumer groups](streams.md) | Application developers | Server-side offsets, consumer groups, range leases, redelivery, dead letters, offset retention, long-poll `watch`, NDJSON/SSE streams, and client examples |
| [Streams design notes](streams-groups-plan.md) | Maintainers | Historical design notes for consumer groups, leases, and offset-based retention |
| [Use cases](use-cases/README.md) | Application developers, architects | Task-oriented patterns built from the primitives, with guarantees, limits and checklists |
| [Financial ledger and payment processing](use-cases/financial-ledger.md) | Application developers, architects | Double-entry ledger, atomic transfers, ordering of execution, idempotent handlers, dead letters, retention, reconciliation |
| [Replacing Redis Streams](use-cases/replacing-redis-streams.md) | Application developers, architects | Concept mapping from `XADD`/`XREADGROUP`/`XACK`, semantics that change your consumer code, migration plan, checklist |
| [Replacing Kafka](use-cases/replacing-kafka.md) | Application developers, architects | Topic/partition/group mapping, atomic multi-stream publish, migration plan, and when to keep Kafka |
| [Asynchronous webhooks](webhooks.md) | Application developers, operators | Webhook rules, async filters and count/sum/avg thresholds, retries, HMAC, dead-letter, GUI |
| [CLI](cli.md) | Operators | Administration, map resize, backup verification, restore drills, inspect, raw requests |
| [Operations](operations.md) | Operators, SRE | Configuration, deployment, security, snapshots, monitoring, capacity, troubleshooting |
| [Docker Compose quick start](docker-compose.md) | Operators, developers | Self-contained Compose stack, auth initialization, GUI, updates, backups, troubleshooting |
| [Raspberry Pi with Docker](raspberry-pi.md) | Operators, edge deployments | 64-bit Raspberry Pi 4/5 setup, SSD data directory, SSH tunnels, updates, troubleshooting |
| [Volume compression](volume-compression.md) | Operators, SRE | ZFS/Btrfs compression, Docker bind mounts and named volumes, verification, benchmarking, migration |
| [Free-tier and low-cost VM deployment](free-tier-deployment.md) | Operators, small deployments | Oracle Cloud, GCP, AWS, DigitalOcean, small profile, Cloud Run read replicas, Spaces backups, restore |
| [Backup and restore](backup.md) | Operators, SRE | Online backup, GUI backup/import, snapshots, verification, restore drills, Docker, vault keys, authorization state, recovery |
| [Branching a database](branching.md) | Operators, CI engineers | Offline clone/branch, reflink/CoW, snapshot sources, CI isolation, limits |
| [Replication and cluster guide](replication-cluster-guide.md) | Operators, SRE | Primary/replica setup, TLS, CLI, GUI, quorum failover, monitoring, backup differences, troubleshooting |
| [Replication design](replication.md) | Maintainers, advanced users | Ordered operation log, apply rules, epochs, snapshots, quorum design, metrics |
| [Benchmarks](benchmarks.md) | Evaluators, maintainers | Environment, methodology, throughput, latency, memory, large-data results |
| [Development](development.md) | Contributors | Repository layout, build, tests, scripts, conventions |
| [Roadmap and limits](roadmap.md) | Everyone | Supported features, deliberate limitations, future work |

## Common tasks

- **Start here:** [Getting started](getting-started.md)
- **Download prebuilt binaries:** [README — Prebuilt binaries](../README.md#prebuilt-binaries)
- **Run on Linux, macOS or Windows:** [README — Quick start](../README.md#quick-start)
- **Run with Docker:** [README — Run with Docker](../README.md#run-with-docker)
- **Run a self-contained Docker Compose stack:** [Docker Compose quick start](docker-compose.md)
- **Run on Raspberry Pi 4/5 with Docker:** [Raspberry Pi with Docker](raspberry-pi.md)
- **Run on a free-tier or low-cost VM:** [Free-tier and low-cost VM deployment](free-tier-deployment.md)
- **Run several databases in one daemon:** [Multiple databases](multi-database.md)
- **Open the local GUI:** [GUI console](gui.md)
- **Pick a client:** [Clients](clients.md) — Python, Rust, Node.js, Go
- **Traverse relationships:** [Graph traversal](graph.md)
- **Connect with a MongoDB driver:** [MongoDB compatibility](mongodb-compatibility.md)
- **Rehearse with MongoDB shadow mode:** [MongoDB wire shadow mode](mongodb-shadow.md)
- **Run a SQL query or simple write:** [SQL queries](sql.md)
- **Import data from another database:** [Migration and import](migration.md)
- **Expose nonodb to Python workers:** [Python client](python-client.md)
- **Run background jobs and schedulers:** [Background jobs](workers.md)
- **Consume change events:** [Outbox events](events.md)
- **React to collection conditions:** [Conditional subscriptions](subscriptions.md)
- **Stream events with a resume token:** [Streaming outbox events](streams.md)
- **Process payments and keep an ordered ledger:** [Financial ledger](use-cases/financial-ledger.md)
- **Deliver changes to an HTTP endpoint:** [Asynchronous webhooks](webhooks.md)
- **Secure a deployment:** [Authentication and authorization](auth.md) and
  [TCP/TLS](operations.md#tcp-and-tls)
- **Run security checks:** [Security checks](security-check.md)
- **Tune writer latency:** [Adaptive writer](adaptive-writer.md)
- **Set up client certificates:** [Mutual TLS](mtls.md)
- **Store sensitive tenant data encrypted:** [Vault collections](vault.md)
- **Expire documents automatically:** [Document TTL](ttl.md)
- **Filter and index timestamps:** [Dates and time zones](dates.md)
- **Store time-series data:** [Time-series collections](timeseries.md)
- **Use general-purpose columnar storage:** [Columnar collections](columnar.md)
- **Compare time-series performance with TimescaleDB:** [Time-series benchmark](timescale-benchmark.md)
- **Store money and exact decimals:** [Numbers and decimals](numerics.md)
- **Use binary, UUID, ObjectId, or `$type`:** [BSON scalar types](bson-types.md)
- **Back up and restore:** [Backup and restore](backup.md)
- **Clone a database for CI:** [Branching a database](branching.md)
- **Set up replication and a cluster:** [Replication and cluster guide](replication-cluster-guide.md)
- **Import a backup as a new database:** [Import wizard](backup.md#import-wizard)
- **Tune timeouts for index builds:** [Timeouts](operations.md#timeouts-and-long-operations)
- **Understand memory usage:** [Capacity and memory](operations.md#capacity-and-memory)
- **Reduce disk usage with filesystem compression:** [Volume compression](volume-compression.md)
- **Reproduce performance numbers:** [Benchmarks](benchmarks.md#reproducing)
