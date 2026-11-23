# Replication design

This page describes the replication design and wire model. For deployment,
configuration, CLI, GUI, failover, and troubleshooting, use the
[Replication and cluster guide](replication-cluster-guide.md).

Replication is separate from [backup and restore](backup.md): backups protect
against history, while replication keeps independent NonoDB nodes close to
users.

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

The daemon writer appends ordered logical operations in the same LMDB
transaction as the data. `changes` carries document payloads; `ops` carries the
exact order of document, metadata, TTL, index, subscription, and outbox
operations.
Older logs without `ops` still apply document changes sequentially. Vault
writes remain outside replication. Set `NONODB_REPLICATION_ENABLED=1` and
`NONODB_REPLICATION_ORIGIN` to enable logging.

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
- Apply metadata and indexes at their exact position in the transaction.
- Apply replicated `_events` outbox documents; do not re-evaluate persistent
  subscriptions or generate a second set of events.
- Generate local document history from the replicated `before`/`after` values
  when the collection history setting is active.
- Notify connection-scoped live streams after commit; never write live events
  into `_events` on the replica.
- Build or backfill indexes on the replica after the metadata operation.
- Expose `promotion_ready`; automatic election refuses to promote a candidate
  that has not applied the last observed source head.
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

Prometheus metrics are exposed at `/metrics/prometheus` with a `metrics:read`,
`admin:db`, or `*:*` token. JSON metrics are available at `/metrics`.

```text
nonodb_replication_enabled
nonodb_replication_role{role="primary|replica|standby"}
nonodb_replication_epoch
nonodb_replication_origin_info{origin="..."}
nonodb_replication_head{database="..."}
nonodb_replication_applied{database="...",origin="..."}
nonodb_replication_applied_epoch{database="...",origin="..."}
nonodb_replication_lag{database="...",origin="..."}
nonodb_replication_ack_nodes{database="...",origin="..."}
nonodb_replication_min_ack{database="...",origin="..."}
nonodb_replication_ack_seq{database="...",origin="...",node="..."}
nonodb_replication_ack_age_seconds{database="...",origin="...",node="..."}
nonodb_replication_transport_errors_total
nonodb_replication_apply_errors_total
nonodb_replication_applied_total
nonodb_replication_pruned_total
nonodb_replication_retention_runs_total
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

Apply is atomic per source transaction and idempotent for the global source
sequence. Replicas append each applied transaction to their own local log, so a
promoted replica can serve downstream replicas. The local log sequence matches
the source sequence. A gap returns an error instead of skipping transactions.
Apply invalidates the replica query cache, and transport/apply failures are
counted and exposed through status/GUI. `/readyz` reports the role and does not
require the writer on a read-only replica.

A promoted replica that should act as a source for other replicas must have
`NONODB_REPLICATION_ENABLED=1` so new local writes are captured. Mixed origins
are supported by the global applied sequence; epochs are tracked per origin.

Before the daemon opens its runtime, a fresh replica can install a consistent
source snapshot:

```text
GET /v1/replication/snapshot
POST /v1/replication/prune   {"through": N}
```

Replicas acknowledge the applied sequence to the source after each successful
batch. The source records acks per node and origin. Prune can require them:

```http
POST /v1/replication/prune
{"through": 1234, "require_acks": true}
```

The effective prune point is capped at the minimum acknowledged replica
sequence. Replicas refresh their acknowledgement while idle, so
`ack_age_seconds` reflects liveness rather than just write activity. If no replica has acknowledged the source, the request fails with
`no_replica_acks`. Set `NONODB_REPLICATION_NODE_ID` on every replica so acks are
distinguishable.

Automatic retention is opt-in:

```env
NONODB_REPLICATION_PRUNE_INTERVAL=60
NONODB_REPLICATION_RETENTION_KEEP=0
```

Every interval the primary prunes through `min_ack - keep`, but never beyond
the current log head. With no replica acks nothing is pruned. Set `keep` above
zero for a safety margin.

`NONODB_REPLICATION_BOOTSTRAP=auto` installs a snapshot when the local database
is fresh, or when the source log no longer contains the next sequence the
replica needs. `always` installs one on every start; `never` disables bootstrap.
The snapshot's own replication head becomes the initial applied sequence, so
changes after that point are pulled normally.

The gap check runs at startup, before the runtime opens. A pruned source log
therefore installs a fresh snapshot on the next start. If a gap appears while
the daemon is running, the pull loop reports an error. With
`NONODB_REPLICATION_EXIT_ON_GAP=1` it also requests a controlled restart with
exit status 75; a
supervisor such as systemd, Docker, or Kubernetes restarts the replica and the
startup gap check installs a fresh snapshot. Without a supervisor, restart the
replica manually.

A different failure, a sequence mismatch or stale epoch, means the local
replication log diverged from the source. The replica then performs an in-place
hard resync: it recreates the affected database and pulls the source log again
without restarting the process. This is not a zero-downtime snapshot restore;
serving reads pause while the database is reset. A hot snapshot resync while
the daemon keeps serving reads is still future work.

### Changing the source

The active pull source is reloaded from the hidden node metadata collection
`<NONODB_DATA>/.node` on every loop. Update it without restarting:

```http
PUT /v1/replication/source
{
  "source": "https://new-primary:7878",
  "databases": ["default", "app1"],
  "interval_secs": 1,
  "bootstrap": "auto"
}
```

Env values seed a new installation; the persisted source wins afterwards.
Role, source, vote, lease, and peer-token state live in the hidden LMDB
collection `<NONODB_DATA>/.node`; legacy JSON files are migrated once. Token,
CA, client certificate, and key remain environment settings. If the source is
unreachable, the replica queries `NONODB_REPLICATION_SEEDS`, voters, and
followers at `/v1/replication/primary` and switches to the reported primary. The new source
must expose a compatible sequence/epoch stream, or the replica must be restarted
so startup bootstrap can install a fresh snapshot.

#### Write routing

`GET /v1/replication/primary` returns the best-known write endpoint:

```json
{
  "role": "replica",
  "epoch": 2,
  "write_url": "https://new-primary:7878"
}
```

The same value is exposed as `write_url` in replication status and
`/readyz.replication.write_url`. A load balancer or client can poll a stable
node and route writes to this URL. The endpoint requires `admin:db`; `/readyz`
is unauthenticated for lightweight health routing.

For write pools, use:

```text
GET /readyz/write
```

It returns `200` only on a writable primary. A replica returns `503` and sets
`Location` to the known primary URL. A write request that reaches a replica and
fails with `read_only` also includes the same `Location` header and
`error.write_url` in the response body, so clients and proxies can retry against
the current primary. The official clients support `write_url`/`auto_primary` for
this; see [Clients](clients.md#routing-writes-to-the-primary). For example, an
HAProxy backend check can
use `option httpchk GET /readyz/write` with `http-check expect status 200`, so
only the current primary is selected for writes.

## Manual role changes

Roles are persisted in the hidden node metadata collection
`<NONODB_DATA>/.node`. The env var seeds a new installation; after that the
persisted role wins.

```http
POST /v1/replication/promote   {"expected_epoch": 0}
POST /v1/replication/demote    {"expected_epoch": 1, "role": "replica"}
```

Each successful change increments `epoch`. A stale `expected_epoch` returns
`409` to prevent an operator acting on outdated role information. Promotion
stops the replica pull loop and enables the writer. Demotion makes the node
read-only again.

When `NONODB_REPLICATION_VOTERS` is set, promotion first requests votes from a
majority of the configured voter URLs:

```env
NONODB_REPLICATION_VOTERS=https://eu-1:7878,https://eu-2:7878,https://eu-3:7878
NONODB_REPLICATION_VOTER_TOKEN=<shared-admin-token>
```

Each voter persists the highest granted `(epoch, candidate)` in the hidden node
metadata collection; conflicting candidates at the same epoch are rejected.
Promotion fails with `quorum_not_reached` without a majority.

A primary with voters configured renews a lease every
`NONODB_REPLICATION_LEASE_INTERVAL` seconds. Voters persist the lease in the
hidden node metadata collection. If the primary cannot reach a majority for
`NONODB_REPLICATION_LEASE_TTL` seconds, it self-demotes to `replica` and stops
accepting writes. This fences an old primary that is partitioned from the
quorum.

Automatic election is opt-in on selected replicas:

```env
NONODB_REPLICATION_AUTO_FAILOVER=1
NONODB_REPLICATION_CANDIDATE=1
NONODB_REPLICATION_CANDIDATE_PRIORITY=0
NONODB_REPLICATION_ELECTION_INTERVAL=5
NONODB_REPLICATION_FAILOVER_TIMEOUT=30
NONODB_REPLICATION_ADVERTISE_URL=https://eu-2:7878
```

After the source has been unavailable for the timeout, a candidate calls the
same quorum/fence promotion path. Voters reject a second candidate at the same
epoch, so only one candidate can obtain a majority. Set the candidate flag only
on replicas that should be allowed to lead. Lower
`NONODB_REPLICATION_CANDIDATE_PRIORITY` values campaign earlier; quorum voting
still ensures that only one candidate at a given epoch can win. After a
successful promotion, the
new primary pushes itself as the configured source to every voter URL and every
URL in `NONODB_REPLICATION_FOLLOWERS`, so both groups reload their pull source
automatically. Followers do not vote, so use them for read replicas that must
not participate in quorum decisions.

When `NONODB_REPLICATION_ADVERTISE_URL` is set, promotion then asks the current
source to demote itself with this node as `successor`. The promotion is aborted
if that fence request fails; use `"force": true` (CLI `--force`) only when the
old primary is unreachable and you accept the split-brain risk. `force` does not
bypass quorum voting.

Demote accepts an optional successor URL:

```http
POST /v1/replication/demote
{"expected_epoch": 2, "role": "replica", "successor": "https://new-primary:7878"}
```

Replicas pulling from a non-primary source with a successor automatically
persist that successor as their new source and reload the pull loop. Promotion
clears the successor.

This is **manual** promotion, not quorum failover. The old primary must be
demoted or stopped to avoid split-brain.

Replication transactions carry the writer's role epoch. A replica records the
highest applied epoch per origin and rejects any transaction from an older
epoch with `stale replication epoch`. Promoting a source therefore fences
replicas against transactions produced before the promotion, as long as they
pull from the demoted node under the same origin.

Lease-gated writes, quorum promotion, leases, candidate election, and the
cluster control plane are implemented. See the
[replication and cluster guide](replication-cluster-guide.md) for operation.

## Cluster control plane

The current primary publishes one hidden `__nonodb_control` document through the
normal durable replication log. It contains:

- the auth registry snapshot (principals, scopes, token hashes, certificate
  fingerprints);
- backup job definitions and run state;
- the non-default database catalog;
- the cluster peer token used for cross-node source/vote authentication.

Replicas apply the snapshot immediately, so every node has the same admin
credentials and configuration. Write-side owners — TTL sweeper, history pruning,
subscription evaluation, automatic events, backup scheduler, retention, and
maintenance — only run while the node is a writable primary.

Promotion readiness requires both the data head and the control-plane snapshot.
A candidate that is behind either one refuses automatic promotion.

The cluster peer token is stored in the control document and in the hidden
local metadata collection `<NONODB_DATA>/.node`. This is a deliberate cluster-wide
administration model: every fully joined node can become primary and has the
credentials needed to accept clients and follow the next primary.

Database create/drop is part of the catalog. Replicas create missing databases
and, by default, drop databases that disappear from the catalog. Set
`NONODB_CLUSTER_PRUNE_DATABASES=0` to keep removed local databases.

Vault payloads are replicated as encrypted envelopes. Tenant keys are
client-supplied and remain outside the control plane.

## Implementation phases

### Phase 1 — log and status

- [x] Core replication record and `_replication` sub-database.
- [x] Append/read/prune core primitives and tests.
- [x] Writer integration for normal CRUD, atomic write batches, and TTL expiry.
- [x] Durable transaction sequence per database.
- [x] Read-only replication status and changes endpoints.
- [x] Writer replication integration test.
- [x] SQL write capture.
- [x] Vault encrypted-envelope write capture.
- [x] Role, capture, origin, and head metrics.
- [x] Applied sequence and lag metrics.
- [x] Transport and apply-error counters.
- [x] Cache invalidation after replica apply.

### Phase 2 — primary → replica

- [x] Pull transport with HTTP, optional TLS, and optional client mTLS.
- [x] Apply path with per-origin `(origin, seq)` idempotence.
- [x] Read-only replica write gating.
- [x] Applied sequence and lag status/metrics.
- [x] Snapshot bootstrap with recorded sequence.
- [x] Replicas keep a local log of applied transactions for promotion.
- [x] Global applied sequence supports mixed origins.
- [x] Epoch fencing and source changes.
- [x] Automatic snapshot resync at startup when the source log was pruned too far.
- [x] Graceful restart on a runtime gap when `NONODB_REPLICATION_EXIT_ON_GAP=1`.
- [x] In-place hard resync when the local log diverges from the source.
- [ ] Hot snapshot resync while the daemon keeps serving reads.
- [x] Replica readiness policy includes data and control-plane head.

### Phase 3 — operations and GUI

- [x] Replication tab with role, source, bootstrap, applied/lag, transport
      health, and recent log entries.
- [x] Role-aware `/readyz` and `nonodb-cli replication` status/changes/prune.
- [x] Peer wizard with one-time pairing codes and full role banner.
- [x] Persist and reload the pull source without restart.
- [x] Add peer pairing, revoke, source validation, and control-plane snapshot.
- [x] Pause/resume/disconnect controls and in-place hard resync.
- [x] Manual promote/demote with persisted role, expected epoch, and audit.
- [x] Promotion/demote confirmation workflow in the GUI.
- [ ] Certificate expiry and lag dashboards.
- [ ] Alert rules.

### Phase 4 — failover and stronger policies

- [x] Lease-based promotion and lease-gated writes.
- [x] Fencing epochs.
- [ ] Synchronous quorum writes / zero-RPO standby.
- [ ] Per-collection consistency profiles.
- [ ] Quorum payments design.

## Open questions

- Which writer operations are replicated: document writes only, or also indexes,
  TTL, history, and collection settings?
- Do replicas generate local history/events, or receive them?
- How long are tombstones and replication records retained?
- Should the replication API be on the same listener or a dedicated port?
- How are vault collections keyed across regions?


## GUI

The GUI **Replication** tab explains the role, epoch, source, bootstrap, lag,
applied positions, acknowledgements, and write routing for the connected node.
The **Cluster** tab probes every node listed with `--cluster` and shows the
whole topology, including offline members.

![Replication tab](assets/gui/replication.png)

![Cluster tab](assets/gui/cluster.png)

## Events, watch, and streams during replication

There are three different mechanisms:

1. **Replication log** — every committed write is appended as an ordered set of
   logical operations: document changes, collection metadata, per-document TTL
   metadata, indexes, subscriptions, and outbox events.
2. **Application events** — when enabled, the primary writes event documents to
   `_events`. Those documents are replicated as `event_append` operations in the
   same transaction. Subscription definitions are replicated as metadata.
3. **Watch/streams** — long-poll `watch`, NDJSON, and SSE read replicated
   `_events`; connection-scoped `mode:"live"` notifications are dispatched by
   the replica apply path after commit.

On a replica:

- replicated writes produce the same `_events` documents as the primary;
- long-poll `watch` on `_events` sees replicated events;
- live `/v1/watch`, NDJSON, and SSE receive replicated document changes;
- persistent subscriptions are **not** re-evaluated on the replica, avoiding
  duplicate outbox events; they remain active for when the node is promoted;
- the local replication log is still available at
  `GET /v1/replication/changes` for checkpoint-based consumers.

## Replica feature support

The replica remains intentionally read-only for clients. Reads and administrative
inspection work; client writes are rejected with `read_only`.

| Feature | On a replica | Notes |
|---|---|---|
| `find`, `find_one`, `count`, `aggregate`, SQL `SELECT` | Yes | Replicated documents are readable. |
| `insert`, `update`, `delete`, `batch`, SQL writes, import | No | Rejected with HTTP 403 `read_only`. |
| Primary `_id` lookup | Yes | `_id` is built into every collection. |
| Collection creation, TTL, history, event settings | Yes | Replicated as ordered metadata operations. |
| Secondary, text, fuzzy, vector, geo indexes | Yes | Created and backfilled on the replica; subsequent applies maintain them. |
| Unique index enforcement | Yes | The definition is replicated, so a promoted replica keeps enforcing it. |
| TTL | Config and metadata replicated | Insertion and date-field settings replicate. Expiry runs on the primary and is replicated as a delete; date-field backfill replicates exact per-document expiry so strict reads match before any sweep. The replica does not run a local TTL sweeper. |
| Document history | Yes | Generated locally from replicated before/after values and the replicated history setting. History pruning is paused while read-only and resumes after promotion. |
| Automatic collection events | Yes | `_events` documents are replicated atomically with the source transaction. |
| Conditional subscriptions | Yes | Definitions and renewals are replicated; evaluation happens only on the primary. |
| Long-poll `watch` on `_events` | Yes | Replicated outbox documents are readable. |
| Live `/v1/watch`, NDJSON, SSE, thresholds | Yes | Apply dispatches local live notifications after commit. |
| Background jobs / workers | Must target primary | Workers claim jobs with writes; a replica rejects the claim. |
| Backup, verify, export | Yes | Read-only operations over local database files. |
| Metrics, readiness, GUI monitoring | Yes | Metrics and the Replication/Cluster tabs work on replicas. |
| GUI data editing, imports, role-independent writes | No | The same `read_only` gate applies. |
| Multi-database reads | Yes | The replicated catalog adds databases to the source list automatically; explicit `NONODB_REPLICATION_DATABASES` is a seed. |
| Auth registry, admin tokens, certificates | Yes | Replicated as hashes/fingerprints on every node; the same client token works after promotion. |
| Backup jobs and run state | Yes | Replicated; only the primary runs the scheduler. |
| Database catalog | Yes | Create/drop replicated; local databases follow the catalog. |
| Vault payloads | Yes | Encrypted envelopes are replicated; the tenant key remains client-side. |
| Vault keys | No | Never stored by NonoDB. |
| Replication status/changes/acks/promote/demote APIs | Yes | Administrative endpoints remain available on replicas. |

Metadata and outbox operations are idempotent through the applied sequence. A
replica that joins by snapshot receives all metadata present in that snapshot;
metadata created later arrives through the operation log.

## Configuring and monitoring with the CLI

Runtime source and role operations:

```bash
nonodb-cli replication status
nonodb-cli replication primary
nonodb-cli replication changes 0 100
nonodb-cli replication acks
nonodb-cli replication source http://primary:7878 default 1
nonodb-cli replication promote <epoch>
nonodb-cli replication demote <epoch> replica [successor-url]
nonodb-cli replication prune <seq> --require-acks
```

The CLI can change the pull source, inspect lag/applied sources/acks, prune the
log, and perform manual promotion or demotion. Cluster identity, TLS paths,
voters, followers, leases, election timing, retention, and bootstrap defaults are
configured with `NONODB_REPLICATION_*` environment variables at daemon start;
the CLI does not expose commands for those settings. The GUI adds live cluster
probing, progress, and per-node role/source controls, but not full environment
management.

## Multi-host validation

`scripts/replication_cluster_test.py` runs a five-host cluster with continuous
writes while replicas join dynamically. It verifies snapshot catch-up, replica
acks, quorum election, source propagation to voters and followers, and that all
nodes converge after the primary is stopped.

`scripts/replication_load_test.py` runs a seven-host, one-million-document load
with six nodes joining during the load, eight concurrent writers, and two
quorum failovers. It also enforces memory limits and uses small replication
batches so a large transaction does not create an unbounded `/changes`
response.

Replicas advance their persisted role epoch when they observe a newer epoch in
the applied stream. This is required for a second failover: the next candidate
proposes an epoch above the votes cast for the previous primary.

`replication status` also exposes `promotion_ready`. Automatic election requires
the candidate to have applied the last observed source head; a lagging candidate
waits instead of promoting stale data. Manual `promote --force` remains an
explicit operator override.
