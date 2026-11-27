# Financial ledger and payment processing

Money movement needs three things from a database: writes that cannot be lost
or half-applied, an ordering you can reason about, and a way to prove what was
processed. This page shows how to build a double-entry ledger and a payment
consumer on nonodb, what the engine guarantees, and where you must add
application logic.

> **Short version.** Ordering of *writes* and *acknowledgements* is guaranteed.
> Ordering of *execution* is yours to choose: one member per account gives
> strict sequential processing; several members give throughput but concurrent
> effects. Delivery is at-least-once, so every handler must be idempotent.

## What the engine guarantees

| Guarantee | How it is enforced |
|---|---|
| A ledger row and its event are both written or neither | `write_batch` runs in one LMDB write transaction |
| Events appear in commit order, without gaps | `_seq` is allocated by the single writer inside the same transaction |
| A commit cannot skip an unprocessed event | `committed_seq` advances only across a contiguous prefix; a later batch that finishes first waits for the hole |
| A partial acknowledgement never advances the offset | `commit_stream_group` requires `seq == to_seq`, otherwise `commit_out_of_order`, and the batch stays leased |
| A stale worker cannot move the offset | commits require the batch `lease_id`; otherwise `stale_lease` |
| No two members process the same offset | range leases (`from_seq..=to_seq`) are exclusive; an expired range is redelivered with `attempts` incremented |
| Events are not silently dropped by retention | retention is bounded by the slowest `committed_seq`; a group behind the window fails with `stream_truncated` |
| Offset state survives restart and failover | group state is durable and replicated; leases are node-local, so a promoted replica resumes at `committed_seq + 1` and redelivers every range that was claimed but not committed — at-least-once, never a silent skip; fencing rejects stale commits |

## What you must add

| Requirement | Why | How |
|---|---|---|
| Idempotent handler | delivery is at-least-once (crash between effect and commit replays the batch) | unique index on `_seq` or on a business key, or an upsert-style ledger append |
| Ordering policy | several members run in parallel, so effects can interleave | one stream per account, or a single-member group |
| External side effects | a bank transfer is not in the database transaction | write the intent, call idempotently, reconcile |
| Dead-letter handling | a poison event must not stall the ledger, but must not be forgotten | alert on `nonodb_stream_dead_events`, inspect `stream_dead`, fix and `retry_stream_dead` |
| Reconciliation | prove that "processed" really happened | periodic job comparing `committed_seq` and ledger contents against the stream |

## Data model

Amounts are integers in minor units (see [Numbers and decimals](../numerics.md));
never floats. Each account owns one event stream, which gives a strict
per-account order:

```
ledger_accounts        { _id: "acct-42", currency: "EUR", balance_minor: 123456 }
ledger_entries         { _id, account, transfer_id, seq, amount_minor, direction, at }
ev_acct_acct-42        append-only events for account acct-42  (stream name = collection)
ev_acct_acct-07        append-only events for account acct-07
```

```json
{"op":"create_collection","coll":"ev_acct_acct-42"}
{"op":"create_stream","coll":"ev_acct_acct-42","retention":{"max_events":5000000,"min_seq_keep":100000}}

{"op":"create_index","coll":"ledger_entries","field":"idempotency_key","unique":true}
```

`idempotency_key` is the unique guard: either `"<account>:<seq>"` (replay-safe
against redelivery) or a business key such as `"<transfer_id>:<leg>"`.

## Writing a transfer atomically

Both legs and both events go in one `write_batch`, so the books cannot show one
side of a transfer without the other:

```json
{
  "op": "write_batch",
  "ops": [
    {"op": "insert", "coll": "ledger_entries", "doc": {
      "idempotency_key": "tr-9f1:debit",
      "transfer_id": "tr-9f1",
      "account": "acct-42",
      "amount_minor": -25000,
      "currency": "EUR",
      "direction": "debit"
    }},
    {"op": "insert", "coll": "ledger_entries", "doc": {
      "idempotency_key": "tr-9f1:credit",
      "transfer_id": "tr-9f1",
      "account": "acct-07",
      "amount_minor": 25000,
      "currency": "EUR",
      "direction": "credit"
    }},
    {"op": "insert", "coll": "ev_acct_acct-42", "doc": {
      "type": "transfer.debit", "transfer_id": "tr-9f1",
      "amount_minor": -25000, "counterparty": "acct-07"
    }},
    {"op": "insert", "coll": "ev_acct_acct-07", "doc": {
      "type": "transfer.credit", "transfer_id": "tr-9f1",
      "amount_minor": 25000, "counterparty": "acct-42"
    }}
  ]
}
```

A duplicate `idempotency_key` fails the whole batch with `duplicate_key`; that is
the desired outcome for a retried transfer request — nothing is written twice.

Each insert into an `ev_acct_*` collection gets a `_seq` in that account's
stream, so `acct-42` and `acct-07` each have their own monotonic history. The
two accounts are ordered independently; the transfer itself is atomic.

## Consuming (order of execution)

### Strict order per account — the default for money

One member per group per account. The group processes offsets one after another;
`committed_seq` is the durable "everything up to here is applied" marker:

```json
{"op":"create_stream_group","coll":"ev_acct_acct-42","group":"apply","start":"earliest","max_attempts":8}
```

```python
while True:
    batch = call({
        "op": "watch", "coll": "ev_acct_acct-42", "mode": "group",
        "group": "apply", "member": "worker-1",
        "limit": 50, "lease_ms": 30_000, "timeout_ms": 1_000,
    })
    if not batch["events"]:
        continue
    # Apply all events of the batch, then commit the batch as a whole.
    apply_batch(batch["events"])
    call({
        "op": "commit_stream_group", "coll": "ev_acct_acct-42", "group": "apply",
        "seq": batch["to_seq"], "lease_id": batch["lease_id"],
    })
```

Keep `limit` small (1–50) when strict ordering matters: a batch is the unit of
redelivery, so a smaller batch means less work is replayed after a crash. Scale
across accounts, not within one account, by running one worker per account (all
members of the same group receive disjoint ranges only if you allow parallelism,
which is exactly what you do not want here).

### Throughput across accounts

A consumer fleet can run many `(account, group)` pairs in parallel — they are
independent streams. Within each account the order is strict; across accounts it
is not, which is fine because accounts cannot observe each other's order except
through the atomic transfer write.

### If you prefer one global stream

Writing to a shared `_events` collection and processing it with a single-member
group also gives strict total order, but caps throughput at one consumer per
group and mixes all accounts in one log. Use per-account streams unless you
genuinely need global ordering.

## Idempotent handler

The sketch below is pseudo-code: the exact client call differs between the
Python, Rust, Node.js and Go clients, but the transaction shape must not.

```python
def apply_event(event, tx):
    key = f'{event["_seq"]}'                  # or a business key
    entry = {
        "_id": f'apply:{event["type"]}:{key}',
        "transfer_id": event["transfer_id"],
        "seq": event["_seq"],
        "applied_at": now_ms(),
    }
    try:
        tx.insert("ledger_applied", entry, unique_id=True)
    except DuplicateKey:
        return                                 # already applied: safe replay
    # ... update balance / post to the ledger inside the same tx ...
```

Rules of thumb:

- The idempotency record and the balance change must land together.
- Never trust "I called commit" as proof — the commit can fail
  (`stale_lease`, `commit_out_of_order`) and the batch is replayed.
- Treat `commit_out_of_order` as a bug in the handler (you acknowledged the
  middle of a range) or as a lost lease; re-read the batch, do not force it.

## Failure matrix

| Failure point | What happens | What you must do |
|---|---|---|
| Crash before the batch is applied | lease expires, the range is redelivered with `attempts + 1` | nothing, replay is safe |
| Crash after applying, before commit | the range is redelivered | idempotency record makes the replay a no-op |
| Commit rejected with `stale_lease` | another member owns the range now | drop local state, stop working on that range |
| Commit rejected with `commit_out_of_order` | partial ack or a lost lease | fix the handler, let redelivery happen |
| Handler throws repeatedly | after `max_attempts` the range is dead-lettered and the group moves on | alert, inspect `stream_dead`, fix, `retry_stream_dead` |
| Group fell behind retention | next claim fails with `stream_truncated` | rebuild balances from the ledger, then `reset_stream_group` to `earliest` |
| Node fails, replica is promoted | leases were node-local, so every claimed-but-uncommitted range is claimed again from the committed offset | fencing prevents stale commits; replay is idempotent |
| External bank call times out | unknown state | intent + idempotency key at the provider, plus reconciliation |

## Dead letters

```json
{"op":"stream_dead","coll":"ev_acct_acct-42","group":"apply"}
{"op":"retry_stream_dead","coll":"ev_acct_acct-42","group":"apply","from_seq":412901}
{"op":"drop_stream_dead","coll":"ev_acct_acct-42","group":"apply","from_seq":412901}
```

A dead-lettered range does **not** block the group — the committed offset advances
across it, so a single bad event cannot stop an account. That is a deliberate
trade: it moves the failure from "stalled ledger" to "visible exception that
must be handled". Alert on it and retry it once fixed; retrying after the group
has moved on is supported (the replay is acknowledged without rewinding the
offset).

## Retention

Retention never drops events at or above the slowest consumer's `committed_seq`,
so an unfinished account pins its own log. Set a generous window and monitor lag:

```json
{"op":"create_stream","coll":"ev_acct_acct-42",
 "retention":{"max_events":5000000,"max_age_ms":7776000000,"min_seq_keep":100000}}
```

```bash
NONODB_STREAM_PRUNE_INTERVAL=60 NONODB_STREAM_PRUNE_BATCH=1000 nonodb-daemon ...
```

If a group does fall behind and is truncated, rebuild the balance from
`ledger_entries` (the authoritative books), then reset:

```json
{"op":"reset_stream_group","coll":"ev_acct_acct-42","group":"apply","to":"earliest"}
```

## Reconciliation

Run a scheduled job per account and compare three numbers:

1. `stream_stats` → `committed_seq` for the group,
2. the highest `seq` recorded in `ledger_entries` for that account,
3. the balance recomputed by summing `ledger_entries.amount_minor`.

Any mismatch is either an unfinished batch (retry/replay) or a real bug. Because
the stream is append-only and ordered, the diff report can point at the exact
`_seq` where the two views diverge.

## Monitoring

| Signal | Action |
|---|---|
| `nonodb_stream_lag_events > budget` | a consumer is behind or stuck |
| `nonodb_stream_inflight_events` flat and non-zero | a member holds leases without committing (crash or hang) |
| `nonodb_stream_dead_events` increasing | a transfer needs manual handling |
| `nonodb_stream_redeliveries_total` rising with flat lag | `lease_ms` too short for the handler |
| `nonodb_stream_commit_failures_total` rising | stale/partial commits — check handler and lease timeouts |
| `nonodb_stream_truncated_total` moving | retention crossed a group; rebuild and reset |

See [Monitoring and alerting](../monitoring.md#stream-metrics).

## Limits to design around

- **At-least-once, not exactly-once.** No two-phase commit with external
  systems; idempotency keys plus reconciliation are the honest answer.
- **Per-stream order only.** Two accounts (or two streams) have no relative
  order beyond the atomic write that created both legs.
- **No cross-stream transactions.** A `write_batch` is one transaction inside one
  database; use one database per ledger, or accept per-database atomicity.
- **Dead letters break "no skip" by design.** They are visible, retryable, and
  do not block, so a zero-dead-letter policy means monitoring and on-call.
- **Balances are derived, not enforced.** The store keeps documents; the
  invariant "sum of entries equals balance" is your handler's and reconciler's
  job (a unique idempotency key and a single-writer stream make it provable).

## Checklist

- [ ] amounts in integer minor units, one currency per account
- [ ] `write_batch` for both legs and both events
- [ ] unique `idempotency_key` on the ledger
- [ ] one stream per account, one member per group when strict order matters
- [ ] small `limit`, `lease_ms` longer than the worst handler run
- [ ] idempotency record written in the same transaction as the effect
- [ ] `max_attempts` set, dead-letter alerts wired to on-call
- [ ] retention window sized to the longest planned consumer outage
- [ ] reconciliation job with a per-account `committed_seq` check

## Related

- [Streaming outbox events](../streams.md) — offsets, groups, leases, retention
- [HTTP API — streams](../api.md#streams) — exact request/response shapes
- [Outbox events](../events.md) — atomic data + event writes
- [Numbers and decimals](../numerics.md) — money representation
- [Background jobs](../workers.md) — durable jobs with leases, for non-stream work
