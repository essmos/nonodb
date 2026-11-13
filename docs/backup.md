# Backup and restore

This is the operational guide for protecting a nonodb data directory and
recovering it after an incident. It covers online backups, periodic snapshots,
verification, restore drills, Docker, vault collections, and the separate
authorization state.

## What a backup contains

A backup is scoped to one database or to a whole instance. A
`nonodb-cli backup` produces one database backup:

```text
data.mdb       consistent, compacted LMDB environment for one database
manifest.json  source, timestamp, size, and SHA-256
```

A `nonodb-cli backup-all` produces an instance backup:

```text
manifest.json           instance manifest listing every database and SHA-256
default/data.mdb        one directory per database
default/manifest.json
app1/data.mdb
app1/manifest.json
```

Each LMDB environment includes that database's documents, indexes, full-text,
fuzzy, and vector data, history, TTL metadata, collection profiles, and database
slots. Databases are independent: an instance backup is consistent per
database, but it is not an atomic snapshot across databases. A backup does
**not** include:

- `lock.mdb` — it is recreated and is not data;
- `auth.json` — authorization lives outside LMDB and is instance-wide;
- the audit log;
- vault tenant keys.

Backups are consistent copies, not a write-ahead log. There is no point-in-time
recovery to an arbitrary operation. The recovery point is the moment each
database copy was taken.

## Quick reference

| Task | Command |
|---|---|
| Online backup of one database | `nonodb-cli --db app1 backup ROOT OUT` |
| Online backup of all databases | `nonodb-cli backup-all ROOT OUT` |
| Inspect a database backup | `nonodb-cli inspect BACKUP` |
| Verify one database backup | `nonodb-cli verify-backup BACKUP` |
| Deep-check one database directory | `nonodb-cli --db app1 verify ROOT` |
| Restore one database | `nonodb-cli --db app1 restore BACKUP ROOT --force` |
| Restore and verify one database | `nonodb-cli --db app1 restore BACKUP ROOT --force --verify` |
| Restore all databases | `nonodb-cli restore-all BACKUP ROOT --force --verify` |
| Automated drill | `python3 scripts/restore_drill.py` |

`ROOT` is the instance root (`NONODB_DATA`). Without `--db`, single-database
file commands resolve a legacy `data.mdb` root first, then `default/data.mdb`,
then `ROOT/<NONODB_DB>/data.mdb`.

`backup`, `inspect`, `verify`, `verify-backup`, and `restore` open files
directly. They do not use the socket or a bearer token. `restore` requires the
daemon to be stopped; `backup`, `inspect`, and `verify` are read-only.

## Online backup

`nonodb-cli backup` opens one database read-only and writes a compacted copy.
It is safe while the daemon is running:

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ)
nonodb-cli --db app1 backup /var/lib/nonodb "/var/backups/nonodb/$TS/app1"
```

Typical output:

```text
backup: /var/lib/nonodb/app1 -> /var/backups/nonodb/20261109T120000Z/app1/data.mdb
manifest: /var/backups/nonodb/20261109T120000Z/app1/manifest.json (sha256 ...)
```

To protect every database, use the instance command:

```bash
nonodb-cli backup-all /var/lib/nonodb "/var/backups/nonodb/$TS"
```

`backup-all` copies databases sequentially. Each copy is internally consistent,
but writes committed to one database after it was copied are not reflected in
the others. If the application requires a cross-database recovery point, quiesce
writes for the duration of `backup-all`.

`CompactionOption::Enabled` omits free pages, so a backup is usually smaller
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

Each database writes snapshots below its own directory:
`$NONODB_SNAPSHOT_DIR/<database>/data-<unix-ms>.mdb`. Retention is independent
per database and removes the oldest files after each successful snapshot.
Snapshots do not have a `manifest.json`, are not atomic across databases, and
are not automatically verified.

For a one-off restore from a snapshot, select the database and pass the file
directly:

```bash
systemctl stop nonodb
nonodb-cli --db app1 restore \
  /var/backups/nonodb/snapshots/app1/data-1767225600000.mdb \
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

For one database directory without a manifest, use:

```bash
nonodb-cli --db app1 verify /var/lib/nonodb
nonodb-cli --db app1 verify /var/lib/nonodb --json
```

`inspect` gives a quick count-only check without deep verification:

```bash
nonodb-cli inspect /var/backups/nonodb/20261109T120000Z/app1
nonodb-cli inspect /var/backups/nonodb/20261109T120000Z/app1 users
```

An instance backup has one manifest per database plus `manifest.json` at its
root. `verify-backup` validates a database directory, not the instance root, so
verify each `<database>/` subdirectory (or restore with `restore-all
--verify`). There is no separate `verify-all` command yet.

A non-zero exit status from `verify-backup` or `verify` means the backup must
not be used. For large databases, verification is I/O-heavy; schedule it like a
backup.

## Restore

Stop the daemon before replacing any LMDB file:

```bash
systemctl stop nonodb

# One database:
nonodb-cli --db app1 restore \
  /var/backups/nonodb/20261109T120000Z/app1 \
  /var/lib/nonodb \
  --force \
  --verify

# Or every database listed in an instance manifest:
nonodb-cli restore-all \
  /var/backups/nonodb/20261109T120000Z \
  /var/lib/nonodb \
  --force \
  --verify

chown -R nonodb:nonodb /var/lib/nonodb
systemctl start nonodb
```

Rules:

- Never restore over a live database. Replacing the `data.mdb` of any open
  database under a running process is unsafe.
- `--force` is required when the destination `data.mdb` already exists.
- With an instance root, `--db app1` targets `ROOT/app1/data.mdb`. Without
  `--db`, the target is the legacy root or `default/data.mdb`.
- `restore` removes a stale `lock.mdb` from the destination database directory.
- The backup can be a directory containing `data.mdb` or a direct path to an
  `.mdb` file.
- If a directory has `manifest.json`, restore checks its size and SHA-256 before
  copying.
- `--verify` deep-checks the restored database directory immediately after the
  copy.
- `restore-all` restores only databases listed in the instance manifest;
  unrelated database directories are left untouched.
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

- Use `nonodb-cli backup` per database or `nonodb-cli backup-all` for the
  instance, not an arbitrary filesystem copy of a live `data.mdb`.
- Remember that `backup-all` is not atomic across databases; quiesce writes if
  recovery must be consistent across the whole application.
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

Back up every database in a named volume into a host directory:

```bash
mkdir -p /host/backups
docker run --rm \
  -v nonodb-data:/var/lib/nonodb:ro \
  -v /host/backups:/backups \
  ghcr.io/essmos/nonodb:0.20.0 \
  nonodb-cli backup-all /var/lib/nonodb "/backups/$(date -u +%Y%m%dT%H%M%SZ)"
```

For one database, add `--db app1` before `backup` and use a database-specific
output directory.

Restore with the container stopped:

```bash
docker compose -f deploy/docker-compose.yml stop

docker run --rm \
  -v nonodb-data:/var/lib/nonodb \
  -v /host/backups:/backups \
  ghcr.io/essmos/nonodb:0.20.0 \
  nonodb-cli restore-all /backups/20261109T120000Z /var/lib/nonodb --force --verify

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
`nonodb-cli backup` or `backup-all`. They are instance-wide, not per database.
Back them up separately and protect them as credentials:

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
| Accidental document deletion | Restore the latest verified backup for that database to a scratch directory, extract affected data, or restore the database after stopping the daemon |
| Corrupt `data.mdb` in one database | Restore that database's latest backup that passes `verify-backup` |
| Corrupt files in several databases | Restore the instance backup with `restore-all`, or restore each database separately |
| Lost host or disk | Restore the off-host instance copy and reinstall the daemon from the matching release |
| Failed upgrade | Stop the daemon, restore the pre-upgrade database or instance backup, restart the previous binary |
| Lost `auth.json` | Restore its separate protected copy or recreate principals and credentials; database backups do not contain it |
| Lost vault key | Ciphertext is unrecoverable; restore the key from the separate key backup |
| Suspected bad backup | Run `verify-backup` on each database directory; if it fails, use an older verified copy |

## Limitations

- No write-ahead log or point-in-time recovery.
- No incremental backup; each database backup copies the full compacted
  environment.
- `backup-all` is sequential, not an atomic cross-database snapshot.
- LMDB free pages remain in the live file until a compacting backup/restore.
- Snapshot retention does not verify files automatically.
- Cross-collection consistency is the consistency of the LMDB snapshot, not an
  application-level transaction boundary.
- A restored older backup also restores the TTL entries from that point in time;
  the next maintenance sweep removes documents whose expiry has passed.
