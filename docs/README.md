# nonodb documentation

Start with the [project README](../README.md) for the problem nonodb solves,
quick-start instructions, and a feature overview.

## Guides

| Guide | Audience | Contents |
|---|---|---|
| [Architecture](architecture.md) | Maintainers, advanced users | Process model, LMDB layout, write path, indexes, search, crash safety |
| [HTTP API](api.md) | All client authors | Operations, request/response formats, filters, updates, errors |
| [Python client](python-client.md) | Application developers | Connection, CRUD, upsert, indexes, search, batches, cache, FastAPI |
| [CLI](cli.md) | Operators | Administration, backup, restore, inspect, raw requests |
| [Operations](operations.md) | Operators, SRE | Configuration, deployment, security, snapshots, monitoring, capacity, troubleshooting |
| [Benchmarks](benchmarks.md) | Evaluators, maintainers | Environment, methodology, throughput, latency, memory, large-data results |
| [Development](development.md) | Contributors | Repository layout, build, tests, scripts, conventions |
| [Roadmap and limits](roadmap.md) | Everyone | Supported features, deliberate limitations, future work |

## Common tasks

- **Install and run:** [README quick start](../README.md#quick-start)
- **Expose nonodb to Python workers:** [Python client](python-client.md)
- **Secure a deployment:** [Authentication](operations.md#authentication) and
  [TCP/TLS](operations.md#tcp-and-tls)
- **Back up and restore:** [Backup and restore](operations.md#backup-and-restore)
- **Tune timeouts for index builds:** [Timeouts](operations.md#timeouts-and-long-operations)
- **Understand memory usage:** [Capacity and memory](operations.md#capacity-and-memory)
- **Reproduce performance numbers:** [Benchmarks](benchmarks.md#reproducing)
