# Replication design

This page describes the planned multi-region replication model. It is separate
from [backup and restore](backup.md): backups protect against history, while
replication keeps independent NonoDB nodes close to users.

## Goals

- Local reads in every region.
- A simple primary → replica path first.
- A hot standby with controlled failover.
- Same ports and same GUI binary on every node.
- A role-aware GUI that clearly shows primary, replica, standby, lag, and
  failover state.
- Mandatory TLS 1.3 + mTLS for node-to-node replication across providers.
- Replication lag, sequence, certificate expiry, and apply failures in
  Prometheus and the GUI.
- No silent split-brain or last-write-wins default.

## Non-goals for the first version

- Full master-to-master writes to the same document.
- Tenant-aware write routing.
- CRDTs or field-level merge policies.
- Quorum payments in the initial replication slice.
- Network replication over plaintext.

## Topology

Start with:

```text
global router / GeoDNS
        │
        ├── primary region      (all writes)
        ├── hot standby         (sync or async)
        └── read replicas       (local reads)
```

Every node uses the same ports:

```text
7878  daemon API + /v1/replication/*
8787  local GUI proxy
```

Replication is pull-based:

```text
replica ──outbound mTLS──▶ primary
```

This works across NAT and providers without requiring inbound access to every
replica.

## Consistency profiles

| Data | Recommended mode | Failure behaviour |
|---|---|---|
| Profiles, content, feeds | primary + async read replicas | small RPO acceptable |
| Orders | home-region owner + one sync replica | no acknowledged write loss on single-region failure |
| Payments | leader per account/payment + quorum | reject writes without quorum; external ledger reconciliation |

The initial implementation targets the first two. Payments remain outside the
first replication slice until quorum, fencing, and audit semantics are complete.

## Durable replication log

The core now contains the first foundation: `nonodb_core::replication`.

Each database has an internal `_replication` LMDB sub-database. A record stores
one committed write transaction, preserving transaction grouping and the single
writer order:

```json
{
  "seq": 1042,
  "origin": "eu-1",
  "created_at_ms": 1794600000000,
  "changes": [
    {
      "collection": "orders",
      "op": "update",
      "id": "o-123",
      "before": {"_id":"o-123","status":"pending"},
      "after": {"_id":"o-123","status":"paid"}
    }
  ]
}
```

Implemented core primitives:

- `Store::append_replication_txn_in()`
- `Store::replication_after()`
- `Store::replication_head()`
- `Store::prune_replication_through()`

The daemon writer now appends normal CRUD, SQL, atomic batch, and TTL-expiry
changes in the same LMDB transaction as the data. Vault writes and some internal
configuration changes are not yet captured. Set `NONODB_REPLICATION_ENABLED=1` and `NONODB_REPLICATION_ORIGIN` to
enable logging.

Read-only inspection endpoints:

```text
GET /v1/replication/status
GET /v1/replication/changes?after=0&limit=100
```

## Replica apply rules

- Apply one whole replication transaction as one local write transaction.
- Reject older `owner_epoch` values after failover.
- Apply `(origin, seq)` idempotently.
- Preserve deletes with tombstones.
- Do not regenerate local application events during replication apply unless
  explicitly configured.
- Rebuild indexes on the replica.
- Deep-verify snapshots before installing them.

## Bootstrap and resync

1. Create a consistent snapshot with `copy_to_file`.
2. Record the snapshot replication sequence.
3. Import the snapshot into a new database.
4. Start pulling changes after that sequence.
5. Verify lag reaches zero.
6. Only then mark the replica ready for reads.

A resync must be resumable and must not expose a half-installed database.

## Transport security

- TLS 1.3.
- Private CA.
- One certificate per node.
- Optional client certificates for normal API traffic; required for
  `/v1/replication/*`.
- Certificate rotation and expiry monitoring.
- Firewall allow-list between known peers.
- NTP on all nodes.
- Optional WireGuard as defence in depth; not a replacement for mTLS.

The replication identity is a node certificate, not a shared bearer token.

## GUI behaviour

The same GUI runs on every node and reads:

```http
GET /v1/replication/status
```

Primary:

```text
[ PRIMARY ] eu-1 · writes enabled · 2 replicas connected
max lag 0.4 s · last seq 10,422
```

Replica:

```text
[ REPLICA ] us-1 · read-only · source eu-1 · lag 0.8 s
last applied seq 10,418
```

The GUI disables normal write controls on a replica and exposes replication
actions to administrators:

- add peer,
- test connection,
- pause/resume,
- resync,
- promote standby,
- demote primary.

Promote/demote require explicit confirmation and an audit entry. Private keys
are referenced by daemon/container paths and are never uploaded through the
browser.

## Monitoring

Planned metrics:

```text
nonodb_replication_role{node,region}
nonodb_replication_peer_connected{peer,region}
nonodb_replication_lag_seconds{peer,region}
nonodb_replication_lag_bytes{peer,region}
nonodb_replication_last_sent_seq{peer}
nonodb_replication_last_applied_seq{peer}
nonodb_replication_pending_changes{peer}
nonodb_replication_apply_errors_total{peer}
nonodb_replication_retries_total{peer}
nonodb_replication_bytes_total{peer}
nonodb_replication_tls_handshake_errors_total{peer}
nonodb_replication_cert_expiry_seconds{peer}
nonodb_replication_failovers_total
nonodb_replication_conflicts_total
nonodb_replication_bootstrap_progress{peer}
nonodb_replication_log_retention_bytes
```

Alerts:

- peer disconnected,
- lag above policy,
- stalled applied sequence,
- apply error growth,
- certificate expiring,
- no standby,
- failover,
- replication log disk pressure.

`/readyz` will be role-aware: a replica is not ready for reads while
bootstrapping or above its allowed lag threshold; a primary is not ready for
writes without the required standby/quorum.

## Current pull replica

A node with `NONODB_REPLICATION_ROLE=replica` or `standby` rejects client writes
and pulls from `NONODB_REPLICATION_SOURCE`:

```text
GET /v1/replication/status
GET /v1/replication/changes?after=N&limit=500
```

`NONODB_REPLICATION_SOURCE_TOKEN` is optional when the source authenticates the
client certificate. `NONODB_REPLICATION_CA_FILE`,
`NONODB_REPLICATION_CERT_FILE`, and `NONODB_REPLICATION_KEY_FILE` enable mTLS.

Apply is atomic per source transaction and idempotent for `(origin, seq)`.
Replicas record the source head and expose `applied`/`lag` in status and
Prometheus. A gap returns an error instead of skipping transactions. Apply
invalidates the replica query cache, and transport/apply failures are counted.

Snapshot bootstrap, epochs, and promotion are not implemented yet, so this is
not yet a production failover mechanism.

## Implementation phases

### Phase 1 — log and status

- [x] Core replication record and `_replication` sub-database.
- [x] Append/read/prune core primitives and tests.
- [x] Writer integration for normal CRUD, atomic write batches, and TTL expiry.
- [x] Durable transaction sequence per database.
- [x] Read-only replication status and changes endpoints.
- [x] Writer replication integration test.
- [x] SQL write capture.
- [ ] Vault write capture.
- [x] Role, capture, origin, and head metrics.
- [x] Applied sequence and lag metrics.
- [x] Transport and apply-error counters.
- [x] Cache invalidation after replica apply.

### Phase 2 — primary → replica

- [x] Pull transport with HTTP, optional TLS, and optional client mTLS.
- [x] Apply path with per-origin `(origin, seq)` idempotence.
- [x] Read-only replica write gating.
- [x] Applied sequence and lag status/metrics.
- [ ] Snapshot bootstrap.
- [ ] Epoch fencing and source changes.
- [ ] Resync and truncation handling.
- [ ] Replica readiness policy.

### Phase 3 — operations and GUI

- [x] Read-only Replication tab with role, origin, head, and recent log entries.
- [ ] Peer wizard, full role banner, and write-control gating.
- [ ] Add peer, test connection, pause/resume, resync.
- [ ] Promote/demote with confirmation and audit.
- [ ] Certificate expiry and lag dashboards.
- [ ] Alert rules.

### Phase 4 — failover and stronger policies

- [ ] Lease-based promotion.
- [ ] Fencing epochs.
- [ ] Sync standby.
- [ ] Per-collection consistency profiles.
- [ ] Quorum payments design.

## Open questions

- Which writer operations are replicated: document writes only, or also indexes,
  TTL, history, and collection settings?
- Do replicas generate local history/events, or receive them?
- How long are tombstones and replication records retained?
- Should the replication API be on the same listener or a dedicated port?
- How are vault collections keyed across regions?
