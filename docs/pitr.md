# Point-in-time recovery

Point-in-time recovery (PITR) restores a database to a chosen timestamp by
combining a consistent snapshot with the change history retained after that
snapshot. It is per database and opt-in.

## Enable PITR

```ini
NONODB_PITR_ENABLED=1
NONODB_SNAPSHOT_DIR=/var/backups/nonodb/snapshots
NONODB_SNAPSHOT_INTERVAL=300
NONODB_PITR_WINDOW_SECS=604800
NONODB_WAL_SEGMENT_BYTES=64MiB
NONODB_WAL_MAX_BYTES=20GiB
```

With PITR enabled and `NONODB_SNAPSHOT_DIR` unset, snapshots are written below
`<NONODB_DATA>/<database>/snapshots`. Snapshots and WAL history are local to
each database. They are not an instance-wide atomic recovery point.

| Variable | Default | Meaning |
|---|---|---|
| `NONODB_PITR_ENABLED` | `0` | Enables continuous WAL archiving and PITR retention |
| `NONODB_PITR_WINDOW_SECS` | `604800` | Recovery window retained, in seconds (7 days) |
| `NONODB_WAL_MAX_AGE_SECS` | PITR window | Hard maximum age of retained WAL; a shorter value also shortens the effective recovery window |
| `NONODB_WAL_SEGMENT_BYTES` | `64MiB` | Target size of one WAL segment |
| `NONODB_WAL_MAX_BYTES` | `20GiB` | Hard per-database WAL size limit |
| `NONODB_SNAPSHOT_INTERVAL` | `300` | Snapshot interval in seconds |
| `NONODB_SNAPSHOT_KEEP` | `24` | Snapshot count retained when PITR is disabled |

Sizes accept a raw byte count or `KiB`, `MiB`, `GiB`, `KB`, `MB`, `GB`.

## Retention and limits

Retention keeps the oldest snapshot needed by the recovery window and the newest
snapshot. WAL segments are removed only after a successful, fsynced snapshot
covers them. A snapshot without its metadata marker is never used to prune WAL.

At 80% of the size limit, or when the age limit is reached, the daemon requests
a snapshot. At the hard limit new writes are rejected with the `wal_full` error
code instead of silently discarding history. Reads remain available. Once
snapshot retention removes covered segments, writes resume automatically.

If a WAL is newer than the database after an in-place restore, the daemon moves
the superseded WAL aside as `<database>/wal.superseded-<unix-ms>` at the next
start. The restored database starts a new history instead of reusing sequences
from the discarded future.

## Restore to a timestamp

Stop the daemon first. Restore a snapshot at or before the target, then replay
the WAL forward:

```bash
systemctl stop nonodb

nonodb-cli --db app1 restore --to 2026-01-01T12:00:00Z \
  /var/backups/nonodb/snapshots /var/lib/nonodb \
  --wal /var/lib/nonodb/app1/wal \
  --force --verify
```

- `--to` accepts RFC 3339, Unix milliseconds, or Unix seconds.
- The snapshot root contains `<root>/<database>/data-<unix-ms>.mdb` and its
  `.json` metadata, or the snapshot files directly.
- `--wal` may be omitted when restoring in place: the default is
  `<target-database>/wal`. Pass it explicitly when restoring into a new data
  root or when the WAL was copied elsewhere. If the default directory does not
  exist, the snapshot is restored without replay and a warning is printed.
- The command selects only a snapshot at or before the target. A future target
  or a target older than the retained base fails.
- If the WAL has a gap, restore fails instead of returning a database with a
  silently incomplete history.
- `--verify` checks the database after replay, not only the snapshot.

Start the daemon and verify the expected documents, indexes, and search
sidecars. Restoring to a newer timestamp than the retained window is not
possible; take a new snapshot before the window expires.

If the snapshot is already in place and you want the daemon to replay on open,
set `NONODB_PITR_REPLAY_TO` to the target timestamp. Replay runs before the
writer accepts requests and is idempotent across restarts. Restore the matching
snapshot first; a target newer than the current data is not a rollback.

## Metrics and alerts

PITR series are exposed per database:

| Metric | Meaning |
|---|---|
| `nonodb_wal_bytes` | Bytes used by retained WAL segments |
| `nonodb_wal_oldest_age_seconds` | Age of the oldest retained WAL segment |
| `nonodb_wal_last_sequence` | Highest sequence archived in the WAL |
| `nonodb_wal_full` | `1` while the hard limit rejects writes |
| `nonodb_pitr_available` | `1` when a snapshot plus WAL can serve a restore |
| `nonodb_wal_pruned_bytes_total` | Bytes removed by snapshot-covered retention |

Recommended alerts:

```promql
nonodb_wal_full == 1
nonodb_pitr_available == 0 and nonodb_wal_last_sequence > 0
nonodb_wal_bytes > 16 * 1024^3
nonodb_wal_oldest_age_seconds > 7 * 24 * 3600
```

The capacity threshold above is an example for the default 20 GiB limit. Use
80% of the configured `NONODB_WAL_MAX_BYTES`. A busy WAL that stays above 80%
for long indicates that snapshots are not completing or that the recovery
window is larger than the disk budget.

## Scope and limitations

- PITR covers one database at a time. Multi-database restores need one restore
  per database and a coordinated application cutover.
- PITR history does not include `auth.json`, audit logs, vault tenant keys, or
  node-local settings. Back those up separately.
- Search and vector sidecars are taken from the snapshot and rebuilt when
  missing; the WAL carries document and index metadata, not external sidecar
  bytes.
- Do not restore under a running daemon. Use the normal backup/restore drill and
  test recovery regularly.

See [Backup and restore](backup.md), [CLI reference](cli.md#restore), and
[Operations](operations.md#snapshots) for the surrounding workflow.
