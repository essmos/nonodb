# Use cases

Working patterns for building systems on nonodb. Each page covers one problem,
the primitives that fit it, the guarantees you do and do not get, and the
operational checklist.

| Case | Fits | Guarantees used |
|---|---|---|
| [Financial ledger and payment processing](financial-ledger.md) | money movement, double-entry bookkeeping, order of execution | server-side offsets, contiguous commits, lease fencing, idempotent handlers, dead letters, offset retention |
| [Replacing Redis Streams](replacing-redis-streams.md) | moving an existing `XADD`/`XREADGROUP`/`XACK` pipeline | offsets instead of a PEL, range acks, consumer-pinned retention, durable dead letters |
| [Replacing Kafka](replacing-kafka.md) | topics/partitions/consumer groups where a single-writer log is enough | per-key streams, durable group offsets, atomic multi-stream publish, replay by reset |

## Common building blocks

Most cases below are assembled from the same parts:

- **Atomic write + event** — `write_batch` (or a single insert) commits the data
  change and its outbox event in one LMDB transaction, so an event can never
  describe a write that did not happen, or a write that has no event.
- **Server-side offsets** — a stream assigns `_seq` inside the writer's
  transaction, so offsets follow commit order exactly. See
  [Streaming outbox events](../streams.md#consumer-groups-and-durable-offsets).
- **Consumer groups** — durable `committed_seq` per group, in-flight range
  leases, redelivery, dead letters. Several members share one group but never
  receive the same offset.
- **Ordering** — delivery is ordered by `_seq`; commits only advance across a
  contiguous prefix; a partial commit is rejected with `commit_out_of_order`.
- **Idempotency** — delivery is at-least-once, so handlers must be idempotent.
  Every pattern below keys the side effect on `_seq` or on a business id.
- **Retention** — driven by the slowest committed offset, so an unfinished
  consumer pins the log, and a group that fell behind the window gets
  `stream_truncated` instead of silently skipping events.

Write a new page when you have a pattern worth copying; keep the guarantees and
limitations sections honest, because these pages are the contract people build
on.

## Candidate pages

Not written yet; listed so the gaps are visible and can be claimed:

| Candidate | What it would cover |
|---|---|
| Event sourcing and audit trail | per-aggregate streams, projections written in the same transaction, rebuilding a projection by replay, checkpointing `committed_seq` as the audit watermark |
| Multi-tenant isolation | database per tenant vs collection/stream per tenant, quotas, per-tenant consumer groups, backup/restore per tenant |
| Change-data fan-out to many services | one stream, one group per service, per-group lag and retention pinning, rollout order when a new consumer starts at `latest` |
| Sagas and process managers | ordered per-entity transitions, compensation events, timeouts driven by the janitor, dead-letter handling for a stuck saga |
| Scheduled and priority work queues | `_jobs` for retries/delays vs streams for ordered work, leases, cron, priority by multiple queues |
| IoT telemetry and device commands | per-device ordered streams, at-least-once command delivery, `nack` for busy devices, retention per fleet |
| Idempotency and deduplication | unique keys, dedupe windows, replay-safe handlers, reconciling a partially applied batch |
