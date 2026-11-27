# Replacing Kafka: when a single-writer log is enough

Kafka is a distributed commit log with partitioned parallelism, ISR replication
and an ecosystem around it. Most applications that reach for it need far less:
an ordered, durable log, per-consumer offsets, and a way to replay. This page
maps Kafka concepts to nonodb and — more importantly — says when you should keep
Kafka.

## Concept mapping

| Kafka | nonodb | Notes |
|---|---|---|
| topic | stream (collection + stream definition) | stream name is the collection name in this release |
| partition | **per-key stream** (`ev_account_42`, `ev_device_a7`) | ordering is per stream, so pick the stream by your ordering key |
| partition key | document field / collection name | `_seq` replaces the partition offset |
| producer | `insert` / `insert_many` / `write_batch` | `_seq` is assigned inside the writer transaction |
| producer `acks=all` | `durability: "full"` (default) | `relaxed` / `unsafe` trade durability for latency |
| consumer group + `__consumer_offsets` | `create_stream_group` + durable `committed_seq` | offsets live in the database and are replicated |
| `offset commit` | `commit_stream_group` (end of lease, `lease_id`) | contiguous prefix only |
| rebalance protocol | none: members claim ranges; expired leases are redelivered | no stop-the-world rebalance, at the cost of at-least-once |
| `auto.offset.reset` | `start: "earliest" \| "latest" \| {"seq": n}` | per group |
| consumer lag | `nonodb_stream_lag_events{stream,group}` | per group, not per event type |
| `retention.ms` | `retention.max_age_ms` | also bounded by the slowest consumer |
| `retention.bytes` | `retention.max_bytes` (accepted, **not enforced**) | use `max_events` |
| log compaction | not supported | keep derived state in a collection with upsert |
| dead-letter topic | dead letters of the group (`stream_dead`, retry, drop) | durable, does not block the group |
| `Kafka Streams` | consumer + derived collections written in the same process | no exactly-once, so idempotent upserts |
| broker replication / ISR | primary → replica replication of the ordered log | no per-partition ISR or leader election per partition |
| multi-topic atomic produce | `write_batch` across collections | one transaction, both streams' events appear together |
| Kafka Connect | CLI import, HTTP API, clients | no connector framework |

## The feature Kafka cannot give you here: atomic multi-stream publish

In Kafka, producing to two topics is two independent appends. In nonodb a single
`write_batch` writes the documents/events for several collections in one LMDB
transaction:

```json
{
  "op": "write_batch",
  "ops": [
    {"op": "insert", "coll": "ev_account_A", "doc": {"type": "debit", "transfer": "tr-1", "amount_minor": -25000}},
    {"op": "insert", "coll": "ev_account_B", "doc": {"type": "credit", "transfer": "tr-1", "amount_minor": 25000}},
    {"op": "insert", "coll": "outbox", "doc": {"type": "transfer", "transfer": "tr-1"}}
  ]
}
```

Consumers of `ev_account_A` and `ev_account_B` will either both see the
transfer or neither. Ordering *between* the two streams is still not defined —
only within each stream.

## Ordering model

- **Ordering key = stream.** Use one stream per account, device, tenant or
  aggregate. Within a stream, `_seq` is gap-free and in commit order.
- **Parallelism = number of streams.** Many streams can be consumed in parallel
  by many processes; within one stream, a single-member group gives strict
  sequential execution.
- **Fan-out = groups.** Ten services can each have a group on the same stream,
  with independent offsets and lag. Retention is driven by the slowest group.
- **Replay = reset.** `reset_stream_group` to `earliest` or `{"seq": n}` replays
  from the log; no separate replay tooling.

Fan-out is cheaper than in Kafka (no topic duplication, no partition rebalance),
but there is no per-partition parallel consumer *within* one ordering key — that
is the cost of a single ordering key.

## Migration plan

1. **Pick the ordering key.** This is the design decision: `partition key →
   stream`. Topics that need per-key order become a family of streams; topics
   that never needed order can be one stream.
2. **Backfill.** Read each partition from the earliest offset (or the committed
   one) with your existing tooling and `insert_many` into the target stream,
   keeping `topic`, `partition`, `offset` and `key` as document fields. The
   original offsets are useful for deduplication and audit.
3. **Dual-run.** Producers write to both systems; consumers read nonodb in
   shadow mode and compare results/counts against the Kafka pipeline.
4. **Cut over per consumer group.** Offsets are independent, so groups move one
   at a time; keep the Kafka consumer as a fallback for one retention window.
5. **Decommission.** Delete Kafka ACLs/topics after the replay window you
   promised has expired.

What does *not* translate: partitions with millions of keys and a parallel
consumer per partition; consumer-group rebalance semantics; exactly-once
processing; log-compacted changelog topics; the connector ecosystem.

## When to keep Kafka

| Requirement | Why nonodb is the wrong tool |
|---|---|
| Sustained throughput well above one writer (~40k events/s mixed writes on a 4 vCPU test box) | writes are serialized through a single writer per database |
| Terabyte-scale log with replay from days/weeks | retention is bounded by consumer offsets and disk, not built for archival replays |
| Exactly-once stream processing | delivery is at-least-once; you must make handlers idempotent |
| Multi-datacenter log mirroring, ISR, per-partition leaders | replication is primary→replica log shipping, not a per-partition consensus protocol |
| Log compaction as a changelog for state recovery | keep state in a collection and rebuild it from events instead |
| Kafka Connect / Schema Registry / streaming SQL ecosystem | no connector framework; SQL is query-oriented |

## When it is enough

| Requirement | Why nonodb fits |
|---|---|
| Order per entity (account, device, tenant, order) | one stream per key, `_seq` in commit order |
| Several independent consumers of the same events | one group per service, durable offsets, per-group lag |
| Atomic state + event, or atomic multi-stream publish | `write_batch` in one transaction |
| Poison messages must not stall the pipeline | dead letters that do not block, with retry/drop |
| History that operators can query | streams are ordinary collections (filters, indexes, SQL) |
| Crash/restart resilience without a broker cluster | offsets and group state are in the replicated database |

## Operational notes

```bash
NONODB_STREAM_PRUNE_INTERVAL=60 NONODB_STREAM_PRUNE_BATCH=1000 nonodb-daemon ...
```

- Alert on `nonodb_stream_lag_events` per group; a stalled group also pins
  retention.
- `nonodb_stream_dead_events` is your DLQ depth; `nonodb_stream_inflight_events`
  flat and non-zero means leases are held without commits.
- `lease_ms` is the redelivery timeout; `max_attempts` is the dead-letter budget.
- Watch `nonodb_stream_commit_failures_total` during rollouts — it spikes when
  consumers are killed mid-batch.

## Related

- [Streaming outbox events](../streams.md) — semantics and tuning
- [Replacing Redis Streams](replacing-redis-streams.md) — the other migration path
- [Financial ledger](financial-ledger.md) — ordered per-key processing
- [Roadmap and limits](../roadmap.md#deliberate-limitations) — non-goals in writing
