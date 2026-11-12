# Backup and restore

This is the operational guide for protecting a nonodb data directory and
recovering it after an incident. It covers online backups, periodic snapshots,
verification, restore drills, Docker, vault collections, and the separate
authorization state.

## What a backup contains

A `nonodb-cli backup` produces a directory with:

```text
data.mdb       consistent, compacted LMDB environment
manifest.json  source, timestamp, size, and SHA-256
```

The LMDB environment includes documents, indexes, full-text, fuzzy, and vector data,
history, TTL metadata, collection profiles, and database slots. A backup does
**not** include:

- `lock.mdb` — it is recreated and is not data;
- `auth.json` — authorization lives outside LMDB;
- the audit log;
- vault tenant keys.

Backups are consistent copies, not a write-ahead log. There is no point-in-time
recovery to an arbitrary operation. The recovery point is the moment the copy
was taken.

## Quick reference

| Task | Command |
|---|---|
| Online backup | `nonodb-cli backup DATA OUT` |
| Inspect a backup | `nonodb-cli inspect BACKUP` |
| Verify manifest and data | `nonodb-cli verify-backup BACKUP` |
| Deep-check a data directory | `nonodb-cli verify DATA` |
| Restore | `nonodb-cli restore BACKUP DATA --force` |
| Restore and verify | `nonodb-cli restore BACKUP DATA --force --verify` |
| Automated drill | `python3 scripts/restore_drill.py` |

`backup`, `inspect`, `verify`, `verify-backup`, and `restore` open files
directly. They do not use the socket or a bearer token. `restore` requires the
daemon to be stopped; `backup`, `inspect`, and `verify` are read-only.

## Online backup

`nonodb-cli backup` opens the source read-only and writes a compacted copy. It
is safe while the daemon is running:

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ)
nonodb-cli backup /var/lib/nonodb "/var/backups/nonodb/$TS"
```

Typical output:

```text
backup: /var/lib/nonodb -> /var/backups/nonodb/20261109T120000Z/data.mdb
manifest: /var/backups/nonodb/20261109T120000Z/manifest.json (sha256 ...)
```

`CompactionOption::Enabled` omits free pages, so the backup is usually smaller
than the live `data.mdb`. The copied database file is synced before the command
exits, then `manifest.json` is written. Creating a backup is I/O-heavy on large
databases; schedule it like any other storage job.

Do not run an external `backup` or `inspect` process at the same time as an
online `resize_map` operation. Finish the resize first.

## Periodic snapshots

The daemon can write compacted snapshots on a fixed interval:

```bash
export NONODB_SNAPSHOT_DIR=/var/backups/nonodb/snapshots
export NONODB_SNAPSHOT_INTERVAL=300
export NONODB_SNAPSHOT_KEEP=24
```

Snapshots are named `data-<unix-ms>.mdb` and are consistent copies. Retention
removes the oldest files after each successful snapshot. Snapshots do not have a
`manifest.json` and are not automatically verified.

For a one-off restore from a snapshot, pass the file directly:

```bash
systemctl stop nonodb
nonodb-cli restore \
  /var/backups/nonodb/snapshots/data-1767225600000.mdb \
  /var/lib/nonodb --force --verify
systemctl start nonodb
```

Snapshot retention is not a replacement for independent backups. Keep at least
one copy off the host and verify it with a restore drill.

## Verify before relying on a backup

Run the checksum and deep database verification:

```bash
nonodb-cli verify-backup /var/backups/nonodb/20261109T120000Z
nonodb-cli verify-backup /var/backups/nonodb/20261109T120000Z --json
```

This checks:

- `manifest.json` version, source, timestamp, size, and SHA-256;
- the LMDB schema and collection registry;
- every document's JSON and `_id` consistency;
- secondary index entries against their documents;
- full-text postings, document lengths, and token totals;
- fuzzy trigram posting counts;
- vector index entries against the stored document vectors;
- history and TTL entry counts.

For a backup or restored directory without a manifest, use:

```bash
nonodb-cli verify /var/lib/nonodb
nonodb-cli verify /var/lib/nonodb --json
```

`inspect` gives a quick count-only check without deep verification:

```bash
nonodb-cli inspect /var/backups/nonodb/20261109T120000Z
nonodb-cli inspect /var/backups/nonodb/20261109T120000Z users
```

A non-zero exit status from `verify-backup` or `verify` means the backup must
not be used. For large databases, verification is I/O-heavy; schedule it like a
backup.

## Restore

Stop the daemon before replacing the LMDB file:

```bash
systemctl stop nonodb

nonodb-cli restore \
  /var/backups/nonodb/20261109T120000Z \
  /var/lib/nonodb \
  --force \
  --verify

chown -R nonodb:nonodb /var/lib/nonodb
systemctl start nonodb
```

Rules:

- Never restore over a live database. Replacing `data.mdb` under a running
  process is unsafe.
- `--force` is required when the destination `data.mdb` already exists.
- `restore` removes a stale `lock.mdb` from the destination.
- The backup can be a directory containing `data.mdb` or a direct path to an
  `.mdb` file.
- If a directory has `manifest.json`, restore checks its size and SHA-256 before
  copying.
- `--verify` deep-checks the restored directory immediately after the copy.
- Ensure the restored files are owned by the daemon user.

After starting the daemon, validate counts, indexes, and a few application
queries:

```bash
export NONODB_SOCK=/run/nonodb/nonodb.sock
nonodb-cli count users '{}'
nonodb-cli indexes users
nonodb-cli find users '{}' --limit 5
```

## Restore drills

A backup is not proven until it has been restored. Run the automated drill on a
schedule:

```bash
cd /opt/nonodb
NONODB_DAEMON=./target/release/nonodb-daemon \
NONODB_CLI=./target/release/nonodb-cli \
python3 scripts/restore_drill.py
```

The drill creates a temporary source database with documents, secondary, text,
fuzzy, history, and TTL state, takes a live backup, verifies the manifest and
index structure, restores with `--verify`, starts a daemon on the restored
directory, and checks counts, indexed ranges, full-text/fuzzy/vector search, history,
TTL, and unique-index enforcement.

Weekly cron example:

```cron
17 3 * * 0 cd /opt/nonodb && NONODB_DAEMON=./target/release/nonodb-daemon NONODB_CLI=./target/release/nonodb-cli python3 scripts/restore_drill.py >>/var/log/nonodb-restore-drill.log 2>&1
```

A failed drill should page an operator. Test restores into a scratch directory
also with `inspect`, `verify`, and a few real application queries.

## Backup strategy

A practical policy:

- Use `nonodb-cli backup`, not an arbitrary filesystem copy of a live
  `data.mdb`.
- Keep at least one verified copy off the host and in a different failure
  domain.
- Retain several recovery points, for example daily copies for two weeks,
  weekly copies for two months, and monthly copies for a year.
- Encrypt backups at rest if they contain production data. Vault backups still
  need their tenant keys protected separately.
- Run `verify-backup` after creating a backup and run a restore drill regularly.
- Take a fresh backup before upgrades, map resizes, and risky maintenance.
- Record the backup timestamp and source version in the incident or deployment
  log.
- Monitor free space in the backup destination. A full disk can leave an
  incomplete backup directory.

Do not copy only the live `data.mdb` with `cp` while the daemon is writing.
LMDB may be in a valid state, but a raw filesystem copy is not the supported
consistent-backup path.

## Docker

Back up a named volume into a host directory:

```bash
mkdir -p /host/backups
docker run --rm \
  -v nonodb-data:/var/lib/nonodb:ro \
  -v /host/backups:/backups \
  ghcr.io/essmos/nonodb:0.14.0 \
  nonodb-cli backup /var/lib/nonodb "/backups/$(date -u +%Y%m%dT%H%M%SZ)"
```

Restore with the container stopped:

```bash
docker compose -f deploy/docker-compose.yml stop

docker run --rm \
  -v nonodb-data:/var/lib/nonodb \
  -v /host/backups:/backups \
  ghcr.io/essmos/nonodb:0.14.0 \
  nonodb-cli restore /backups/20261109T120000Z /var/lib/nonodb --force --verify

docker compose -f deploy/docker-compose.yml up -d
```

The image runs as the unprivileged `nonodb` user. Keep the data volume and the
backup directory permissions compatible with that user.

## Vault collections

Vault backups contain ciphertext plus `_id`, `tenant_id`, and `kid`. They are
not readable without the tenant keys:

- back up keys separately from the database;
- use a secret manager and a documented recovery procedure;
- test key recovery as part of the restore drill;
- losing the key means losing document bodies, although `_id` and `tenant_id`
  remain.

See [Vault collections](vault.md#cache-snapshots-and-backups).

## Authorization and audit state

`auth.json` and the audit log are outside LMDB and are **not** part of
`nonodb-cli backup`. Back them up separately and protect them as credentials:

```bash
install -m 0640 -o root -g nonodb auth.json /var/backups/nonodb/auth.json
```

Restoring an older database must not resurrect an old token or certificate.
That separation is intentional. If `auth.json` is lost, restore it from its
separate protected backup or recreate principals and tokens with
`nonodb-cli auth`.

See [Authentication and permissions](auth.md#backups).

## Recovery scenarios

| Scenario | Recovery |
|---|---|
| Accidental document deletion | Restore latest verified backup to a scratch directory, extract affected data, or restore the full database after stopping the daemon |
| Corrupt `data.mdb` | Restore the latest backup that passes `verify-backup` |
| Lost host or disk | Restore the off-host copy and reinstall the daemon from the matching release |
| Failed upgrade | Stop the daemon, restore the pre-upgrade backup, restart the previous binary |
| Lost `auth.json` | Restore its separate protected copy or recreate principals and credentials |
| Lost vault key | Ciphertext is unrecoverable; restore the key from the separate key backup |
| Suspected bad backup | Run `verify-backup`; if it fails, use an older verified copy |

## Limitations

- No write-ahead log or point-in-time recovery.
- No incremental backup; each backup copies the full compacted environment.
- LMDB free pages remain in the live file until a compacting backup/restore.
- Snapshot retention does not verify files automatically.
- Cross-collection consistency is the consistency of the LMDB snapshot, not an
  application-level transaction boundary.
- A restored older backup also restores the TTL entries from that point in time;
  the next maintenance sweep removes documents whose expiry has passed.
