# Streams with durable offsets and consumer groups — implementation plan

Status: phase 1 implemented (server-side offsets, groups, lease-fenced commits).
Phases 2-4 remain. Target release: after `v0.34.0`.

This document records the design decision for turning the outbox-and-watch
machinery into a stream with server-side offsets, consumer groups, and
size/offset-based retention, and explains why the chosen approach reuses
existing daemon primitives instead of adding a second ordering log.

## Decision

Implement **option A**: offsets on the existing `_events`-style append-only
collections, with group state in LMDB metadata and in-flight tracking modelled
on the `_jobs` lease/fencing pattern.

Rejected alternative (**option B**, a public stream over the internal
`_replication` log) is documented under
[Alternatives](#alternatives-considered) together with the reasons.

## Goals

1. A consumer can commit an offset to the server and resume from it after a
   crash or a restart, from any client, in any language.
2. Several processes can share the work of one stream: each event is delivered
   to exactly one member of a group, and the group makes progress together.
3. Retention can be expressed in events/bytes/offsets, and the daemon never
   deletes data that a live group has not committed, unless the operator
   accepts a truncation and the group is told.
4. Operators can see per-group lag, in-flight counts, redeliveries, and dead
   letters, and can reset a group, from CLI, API, `/metrics`, and GUI.
5. Nothing in the single-writer model changes. No new storage engine, no new
   ordering log, no user code in the writer.

## Non-goals

- Kafka protocol compatibility, `Connect`, `Streams`, KSQL.
- Partitioned topics with per-key ordering (see P4; deferred until a real
  multi-tenant case exists).
- Compacted streams (latest-value-per-key) and `read_committed` isolation.
- Synchronous replication, ISR, or `min.insync.replicas`.
- Binary framing or compression on the stream transport.
- Running handlers inside the daemon.

## What already exists (measured, `v0.34.0` + #18)

| Capability | Where | State |
|---|---|---|
| Append-only event collection, atomic data + event write | `EventProducer`, `write_batch`, `set_collection_events` | done |
| Tail by `_id` cursor, long-poll and continuous NDJSON/SSE | `watch`, `/v1/watch` (`nonodb-daemon/src/main.rs`) | done |
| Server-side filtering (type/filter/projection/threshold) | `watch` request fields | done |
| Client-side checkpoints, at-least-once, overlap rescan | `_event_checkpoints`, `EventConsumer` | done |
| Writer wake-up for streams (no 50 ms poll latency) | `live::stream_notify`, merged in #18 | done |
| Drain throughput, single stream | 135–169k ev/s, 4 vCPU / ZFS | measured |
| Delivery latency p50/p99 | ~1.6 ms / ~2.3 ms | measured |
| Ordering: one global commit order per database | single writer | done |
| Durable lease/fencing/backoff/requeue | `_jobs` (`docs/workers.md`) | done, other subsystem |
| Durable delivery records with retries and dead letters | `_webhook_deliveries` (`docs/webhooks.md`) | done, other subsystem |
| Monotonic sequence, head, acks, prune-by-min-ack | `_replication` (`nonodb-core/src/replication.rs`, `store.rs`, `retention.rs`) | done, internal |
| Retention by age, compaction, rollups | `timeseries` profile | done, other profile |
| Replication of metadata | `ReplicationOp` variants, control plane | done |

The gap is therefore mostly *interface and policy*, not engine: there is no
public offset, no group, and no retention rule that considers a consumer.

## Data model

All new state lives in ordinary LMDB sub-databases owned by the daemon, and is
replicated like other metadata.

### `_streams` (definitions)

```json
{
  "_id": "events",
  "coll": "_events",
  "seq_field": "_seq",
  "retention": {
    "max_events": 1000000,
    "max_bytes": null,
    "max_age_ms": 604800000,
    "min_seq_keep": 0
  },
  "created_at": 1762800000000
}
```

- A stream is a named, ordered view over one append-only collection.
- `_events` gets a `_seq` field, a strictly increasing `u64` allocated by the
  single writer from the same counter as the replication sequence
  (`REPLICATION_SEQ_KEY`), so a stream offset and a replication sequence never
  disagree about order.
- `_seq` is written only for collections that have a stream definition, so a
  plain collection pays nothing.
- `_seq` is exposed on read, and is reserved: user writes that set it are
  rejected with `invalid_query`.

### `_stream_groups` (committed offsets)

```json
{
  "_id": "events:order-sync",
  "stream": "events",
  "group": "order-sync",
  "committed_seq": 412900,
  "members": ["worker-1", "worker-2"],
  "state": "active",
  "truncated_below": null,
  "created_at": 1762800000000,
  "updated_at": 1762800123000
}
```

- `committed_seq` is the highest offset the group has fully processed.
- `state` is `active`, `idle`, or `truncated`.
- `truncated_below` is set when retention removed events above the group's
  `committed_seq`; from then on, `watch` for that group fails with
  `stream_truncated` until the group is reset explicitly. This is the same
  contract as the current `truncated`/`oldest` fields, but it is now a durable
  property of the group rather than an inference per request.

### `_stream_leases` (in-flight, the pending list)

```json
{
  "_id": "events:order-sync:412901",
  "stream": "events",
  "group": "order-sync",
  "seq": 412901,
  "member": "worker-1",
  "lease_id": "01J...",
  "lease_until": 1762800183000,
  "attempts": 1,
  "status": "leased"
}
```

- One document per in-flight batch, not per event: a lease covers a contiguous
  `[from_seq, to_seq]` range, so claiming 500 events costs one write.
- `status` is `leased` or `dead`. Dead leases are the dead-letter queue; they
  are not redelivered automatically and can be inspected, retried manually, or
  dropped.
- Expired leases are requeued by the same janitor pattern as `_jobs`
  (`lease_id` fencing prevents a stale member from committing a batch it no
  longer owns).

### Indexes

```json
{"coll": "_stream_leases", "fields": ["status", "lease_until"]}
{"coll": "_stream_leases", "fields": ["stream", "group", "status", "seq"]}
```

## API

New operations on `/v1/exec` and, for delivery, `/v1/watch`. Field names follow
the existing request style.

| Operation | Purpose | Scope required |
|---|---|---|
| `create_stream` | define a stream, its retention, and filter defaults | `admin:<coll>` |
| `drop_stream` | remove a definition (data stays) | `admin:<coll>` |
| `get_stream` / `list_streams` | inspect definitions | `read:<coll>` |
| `create_group` | create a group at `earliest`, `latest`, or an explicit offset | `admin:<coll>` |
| `reset_group` | move `committed_seq`, clear leases, clear `truncated` | `admin:<coll>` |
| `delete_group` | remove a group and its leases | `admin:<coll>` |
| `stream_stats` | per-group `committed_seq`, `head_seq`, `lag`, `inflight`, `dead`, `oldest_seq` | `read:<coll>` |

Delivery: extend `watch` with `mode: "group"`.

```json
{
  "op": "watch",
  "coll": "_events",
  "mode": "group",
  "group": "order-sync",
  "member": "worker-1",
  "limit": 500,
  "lease_ms": 60000,
  "max_attempts": 5,
  "heartbeat_ms": 10000,
  "filter": {"payload.tenant": "acme"}
}
```

Semantics:

- The server claims a contiguous batch above `committed_seq` that is not
  already leased, writes one lease document, and streams the events with their
  `_seq`.
- The client acknowledges with `commit {stream, group, member, lease_id, seq}`
  after successful processing. A commit advances `committed_seq` only when it
  acknowledges exactly the contiguous prefix; out-of-order or partial commits
  advance nothing and are rejected with `commit_out_of_order`.
- `nack {lease_id, retry_in_ms}` releases the batch for redelivery and
  increments `attempts`. Past `max_attempts`, every event in the batch becomes
  a `dead` lease instead of looping forever.
- If the connection drops, the lease stays until `lease_until` expires, then
  the batch is redelivered to any member. This is at-least-once, unchanged from
  today; handlers must stay idempotent.
- `after`/`cursor` client-side offsets keep working for `mode: "tail"` (today's
  behaviour). Groups are additive; nothing existing changes meaning.

## Retention

`retention` on a stream is enforced by `nonodb-daemon/src/retention.rs`, which
already runs a periodic pass and already computes
`min(replica acks) - keep` for `_replication`.

Rule for a stream:

```text
limit = min(
    max_events,               # newest N events kept
    max_bytes,                # approximate, from LMDB sizes of pruned docs
    max_age_ms,                # newest T events
    min(committed_seq of every group) - min_seq_keep
)
```

`min_seq_keep` is the operator's escape hatch: it allows deletion below a
group's committed offset, which sets `truncated_below` on that group.

Deletion is `seq <= limit`, in bounded batches, inside the writer transaction,
so a consumer never observes a torn view.

## Metrics and observability

Follow the existing `_replication` naming so dashboards stay consistent.

| Metric | Meaning |
|---|---|
| `nonodb_stream_head_seq{stream}` | highest allocated offset |
| `nonodb_stream_committed_seq{stream,group}` | group offset |
| `nonodb_stream_lag_events{stream,group}` | `head - committed` |
| `nonodb_stream_inflight{stream,group}` | leased events |
| `nonodb_stream_redeliveries_total{stream,group}` | leases that expired |
| `nonodb_stream_dead_events{stream,group}` | dead-lettered events |
| `nonodb_stream_truncated_total{stream,group}` | retention crossed a group |
| `nonodb_stream_commit_failures_total{stream,group}` | rejected commits |

CLI:

```bash
nonodb-cli streams list
nonodb-cli streams create _events --max-events 1000000 --max-age 7d
nonodb-cli streams groups _events
nonodb-cli streams reset _events order-sync --to earliest
nonodb-cli streams dead _events order-sync [--retry ID] [--drop ID]
```

GUI: a "Streams" panel next to the existing cluster view, showing lag per group,
in-flight, dead letters, and reset actions.

## Phases

Estimates assume one developer, existing review process, and the current test
suite as the gate.

### Phase 1 — offsets (5–7 days)

- allocate `_seq` in the writer for stream-enabled collections;
- `_streams` and `_stream_groups` metadata, plus replication ops for both;
- `create_stream`, `create_group`, `get_stream`, `list_streams`, `stream_stats`;
- `commit` with prefix validation and `lease_id` fencing;
- `watch mode: "group"` returning `_seq` per event;
- `docs/streams.md` gains a group section; `docs/api.md` gains the new ops.

Acceptance: two clients with one group never see the same event twice in a row
of successful commits; a commit from a stale lease is rejected; restart resumes
from `committed_seq`; `stream_stats` matches a full manual drain.

### Phase 2 — in-flight and redelivery (4–6 days)

- `_stream_leases` with batch ranges, expiry, requeue, `max_attempts`, dead
  letters;
- `nack`, `retry dead`, `drop dead`;
- janitor pass wired into the existing retention tick;
- metrics from the table above.

Acceptance: killing a member mid-batch redelivers after `lease_until` and
increments `attempts`; a poison batch reaches `dead` after `max_attempts` and
stops blocking the group; two members on one group split the stream.

### Phase 3 — retention (2–3 days)

- `retention` fields on streams, pruned with the min-committed rule;
- `truncated_below` propagation and the `stream_truncated` error;
- bounded, batched deletion inside the writer.

Acceptance: a group that stops committing pins retention; `min_seq_keep` below
a group's offset marks it truncated and the next `watch` fails with
`stream_truncated`; no partial visibility across a delete batch.

### Phase 4 — operations (2 days)

- CLI subcommands, GUI panel, `/metrics` wiring, `docs/monitoring.md` entry;
- `docs/roadmap.md`: move streams/groups out of "deliberate limitations" and
  add the remaining non-goals explicitly.

### Deferred — P4, partitions (1 week, only on demand)

`partition_key` with `hash % N`, per-partition offsets and ordering. Note that
under the single writer this buys consumer parallelism, not write throughput.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| `_seq` write and index cost on the hot path | only for stream-enabled collections; append-only index inserts; measure before/after with the existing stream bench |
| Lease churn on small batches | contiguous batch leases (one write per batch), not per event |
| A stalled group pins retention forever | `max_age_ms` and `min_seq_keep` are explicit operator overrides; never silent |
| `min_ack` now has two sources (replicas, groups) | compute one `limit` in `retention.rs` from both, and expose it in `stream_stats` and logs |
| Group state during failover | stream metadata replicates as ops; group rows are ordinary replicated metadata; reject commits from a stale `epoch` the same way replication does |
| Vault collections | streams over vault collections stay rejected, as `watch` does today |
| Double bookkeeping: events in `_events` and transactions in `_replication` | keep both; `_events` is the user payload, `_replication` stays the replication log. Do not merge them |
| Sizing: hundreds of groups on one stream | group offsets are one small document each; leases dominate; document a recommended ceiling and measure |

## Alternatives considered

**Option B — expose `_replication` as the stream.** Attractive because it is
already a durable ordered log with `seq`, `head`, acks, and prune-by-min-ack,
and because a record is a whole transaction (free commit boundaries).
Rejected for now because: records contain vault envelopes, they carry no event
`type`, retention would be shared with replica acks (one slow group can pin the
replication log and therefore replica bootstrap), and it doubles the payload of
every user event. Revisit if per-transaction framing becomes a requirement.

**Per-event offsets in `_replication` without `_events`.** Same objections, plus
it would break the documented outbox model where the event document is
queryable data.

**Redis-Streams-style PEL written from scratch.** Rejected: `_jobs` already
implements leases, fencing, backoff, and expiry recovery, and `_webhook_deliveries`
already implements durable attempts and dead letters. A third mechanism would
triple the maintenance cost for the same semantics.

## Open questions

1. Should a group be able to read events that predate the group's creation
   (`earliest`) or only from creation time (`latest` is the safe default)?
2. Do we expose dead letters as a queryable collection (`_stream_dead`) or only
   through admin ops?
3. Is `max_bytes` worth implementing before someone asks, given that LMDB file
   size does not shrink on delete?
4. Should `stream_stats` include per-type lag (cheap with `type` filters) or stay
   per group only?
