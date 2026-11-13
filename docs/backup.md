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

## Disclaimer

Backup and encryption features are provided **as-is, without warranty**. The
authors and maintainers accept no responsibility for the quality or strength of
encryption, key management, backup correctness, backup failures, restore
failures, or data loss. Use these features entirely at your own risk.

You are responsible for protecting keys and credentials, monitoring jobs, and
regularly verifying backups with real restore tests. Do not rely on a backup
that has never been restored and verified. The GUI requires accepting this
disclaimer before a job can be created or run; acceptance is stored only in the
browser.

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

## Scheduled backups from the daemon and GUI

The daemon can run backup jobs on an interval and export them to a local
directory, S3-compatible storage such as Cloudflare R2, or a restic repository.
Open the **Backups** tab in the GUI to create, edit, delete, or run jobs now.
The **Save & run now** button next to **Create job** saves the current form and
starts that job immediately, which is useful for checking the destination path.

Configuration is stored in `NONODB_BACKUP_CONFIG` (default
`<NONODB_DATA>/backups.json`) with runtime state in `backups-state.json` next to
it. Example:

```json
{
  "version": 1,
  "jobs": [
    {
      "id": "nightly-default",
      "databases": ["default", "app1"],
      "enabled": true,
      "every_minutes": 1440,
      "destination": {
        "kind": "local",
        "path": "/var/backups/nonodb"
      },
      "encryption": {
        "kind": "age",
        "recipient_file": "/etc/nonodb/backup-recipient.txt"
      },
      "keep_last": 14,
      "verify": true,
      "compression": "gzip",
      "exclude_collections": ["audit_scratch", "temporary_cache"]
    }
  ]
}
```

Native jobs can use **Compression → Gzip**. Compression runs before optional
age encryption, so encrypted compressed copies use `data.mdb.gz.age`. Restic
compresses its own repository data, so the compression field applies only to
native local/S3 jobs.

The **Databases** field is a multi-select list of databases known to the daemon.
One job can back up several databases in one run. **Excluded collections** are
removed from every selected database's copy only. The live databases are not
modified. Excluded collections also lose their indexes, history, TTL, events,
and subscription metadata in the copies.

`database` remains supported as a legacy single-database field. New jobs should
use `databases`; when both are present, `databases` wins.

The job copies a consistent compacted snapshot to
`NONODB_BACKUP_STAGING` (default `<NONODB_DATA>/.backup-staging`), optionally
compresses and encrypts it, then copies/uploads it.

The GUI defaults local native backups to `<NONODB_DATA>/backups` and local restic
repositories to `<NONODB_DATA>/restic-repo`. These directories are writable by
the daemon and work in Docker with the data volume. For production, use a
separate mounted volume or off-host destination so a lost data disk does not
also lose the backups. Local layout:

```text
<path>/<job-id>/<timestamp>/data.mdb
<path>/<job-id>/<timestamp>/data.mdb.gz
<path>/<job-id>/<timestamp>/data.mdb.gz.age
<path>/<job-id>/<timestamp>/manifest.json
<path>/<job-id>/<timestamp>/backup-info.txt
```

`backup-info.txt` is a human-readable summary written next to every payload. It
records the database, job, creation time, data file, size, checksum,
compression, encryption, excluded collections, scope, restore commands, and the
same as-is warning shown in the GUI. It is included in local/S3 uploads and in
restic snapshots.

For a job with multiple databases, each database gets its own subdirectory:

```text
<path>/<job-id>/<timestamp>/<database>/data.mdb[.gz][.age]
<path>/<job-id>/<timestamp>/<database>/manifest.json
```

Each database is a separate restic snapshot tagged with both the job and the
database. `keep_last` removes older job timestamp directories after all selected
databases in a run succeed.
S3/R2 retention should use bucket lifecycle rules; scheduled jobs do not delete
remote objects.

### Backup keys in the GUI

Native age backup keys can be generated once from the job form:

1. Enable **age recipient file**.
2. Keep or change the private identity path and public recipient path.
3. Click **Generate key**.

Key files default to `<NONODB_DATA>/backup-keys` and the directory is created
with mode `0700`. Override it with `NONODB_BACKUP_KEY_DIR`. Parent directories
for explicit paths are created on demand, so `/etc/nonodb/...` works only if the
daemon user may write there.

The daemon writes the identity with mode `0600` and the recipient with mode
`0644`, using create-new semantics. Existing files are never overwritten. The
private identity is never returned to the browser and cannot be edited in the
GUI; store it separately from the backup destination and use
`NONODB_BACKUP_IDENTITY_FILE` for restore. A lost identity makes encrypted
backups unrecoverable.

For restic jobs, **Generate** next to the password-file path writes a random
password once with mode `0600`. The password is not displayed or editable in the
GUI. Keep a protected copy of the password file; without it the restic
repository cannot be opened.

### Docker paths

The daemon runs inside the container, so every backup path in the GUI is a
**container path**. A local destination such as `/var/backups/nonodb` must be a
mounted volume, otherwise it writes only into the container filesystem. Key
files also live inside the mounted data volume by default:

```text
container data:  /var/lib/nonodb
keys:            /var/lib/nonodb/backup-keys
local backups:   /var/backups/nonodb   (mount this)
```

Use a separate key volume when the database volume is not backed up with the
key:

```bash
-e NONODB_BACKUP_KEY_DIR=/backup-keys
-v nonodb-backup-keys:/backup-keys
```

For encrypted restore, set `NONODB_BACKUP_IDENTITY_FILE` to the identity path and
mount the identity file into the container. Restic needs no separate age key;
use its password file and repository path.

### S3 and Cloudflare R2

S3 jobs use path-style URLs and SigV4. Example destination:

```json
{
  "kind": "s3",
  "endpoint": "https://<account>.r2.cloudflarestorage.com",
  "bucket": "nonodb-backups",
  "region": "auto",
  "prefix": "prod/nonodb",
  "access_key_env": "AWS_ACCESS_KEY_ID",
  "secret_key_env": "AWS_SECRET_ACCESS_KEY"
}
```

Credentials are read from the named environment variables at upload time and
are never stored in `backups.json` or the GUI. The optional
`session_token_env` supports temporary credentials. Native S3 upload uses a
single PUT and is limited to 5 GiB. The restic engine supports larger
repositories and multipart uploads through restic.

### Restic engine

For encrypted, deduplicated, incremental repositories, choose **Backup type →
Incremental + deduplication (restic)**, then choose **Where → Local directory**
or **S3 / Cloudflare R2** in the GUI. The daemon still creates a consistent
LMDB snapshot first, but then runs restic, which owns chunking,
encryption, deduplication, S3/R2, retention, and restore.

Install restic on the daemon host. The Docker image includes it. Example local
repository:

```bash
install -d -m 0700 /var/backups/nonodb-restic
printf '%s\n' 'choose-a-strong-password' > /etc/nonodb/restic-password
chmod 0600 /etc/nonodb/restic-password
```

Job fields:

```json
{
  "destination": {
    "kind": "restic",
    "repository": "/var/backups/nonodb-restic",
    "password_file": "/etc/nonodb/restic-password",
    "binary": "restic"
  },
  "keep_last": 14,
  "verify": true
}
```

For Cloudflare R2 or S3, set the restic repository to the S3 URL and provide
credentials to the daemon environment:

```text
repository: s3:https://<account>.r2.cloudflarestorage.com/<bucket>/<prefix>
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=auto
```

Restic runs with `RESTIC_PASSWORD_FILE` and creates the repository on first use.
The job tag is `nonodb-job-<job-id>`; retention uses
`restic forget --keep-last N --prune` for that tag, and `verify: true` also runs
`restic check`. The daemon backs up a **non-compacted** consistent snapshot so
restic can deduplicate unchanged pages across runs.

Inspect or restore manually:

```bash
export RESTIC_PASSWORD_FILE=/etc/nonodb/restic-password
restic -r /var/backups/nonodb-restic snapshots
restic -r /var/backups/nonodb-restic restore latest --target /tmp/restore
```

The restored `data.mdb` is under the recorded absolute staging path inside the
target directory. Stop the daemon before replacing a live database.

### Encryption
### Encryption

Generate an age X25519 key pair:

```bash
nonodb-cli backup-keygen /etc/nonodb/backup-identity.txt
# stdout is the public recipient: age1...
```

Put the printed recipient in the job's `recipient_file`
(`/etc/nonodb/backup-recipient.txt`). The private identity stays outside the
daemon and is needed for restore:

```bash
NONODB_BACKUP_IDENTITY_FILE=/etc/nonodb/backup-identity.txt \
nonodb-cli restore /var/backups/nonodb/nightly-default/<timestamp> \
  /var/lib/nonodb --force --verify
```

Encrypted backups use the standard age file format. They can also be decrypted
with the `age` CLI. `verify-backup` currently verifies unencrypted backups;
scheduled encrypted jobs verify the plaintext staging copy before encryption,
and restore checks the plaintext size and SHA-256 from the manifest after
decryption.

### Operational notes

- Only one backup job runs at a time.
- The scheduler runs a due job immediately after start, then polls
  `NONODB_BACKUP_SCHEDULER_INTERVAL` seconds.
- A successful job records last/next run, size, duration, destination, and
  errors in `backups-state.json`.
- Native local/S3 backups are full copies. The restic engine provides
  deduplicated, incremental repositories.
- Ensure staging has room for one copy of the largest database. Native jobs use
  a compacted copy; restic jobs use a non-compacted copy to preserve
  deduplication. Excluding collections adds one compacting pass on staging and
  reduces restic's cross-run deduplication because the filtered copy is
  compacted.
- Scheduled jobs do not replace restore drills.

## Import wizard

The GUI **Import** tab imports a native backup as a **new database** while the
daemon stays online. It never replaces a live database.

Sources:

- **Local folder** on the daemon/container. Paths must be under
  `NONODB_IMPORT_ROOTS` (default `<NONODB_DATA>/backups`).
- **S3 / Cloudflare R2** using `ListObjectsV2` and streaming GET.

The wizard discovers `manifest.json` files, shows database, timestamp, size,
gzip/age/partial status, and the optional `backup-info.txt`. Import then:

1. downloads/copies the payload to staging;
2. verifies size and SHA-256;
3. decrypts age with `NONODB_BACKUP_IDENTITY_FILE` when needed;
4. decompresses gzip when needed;
5. verifies the LMDB database deeply;
6. installs it as the requested new database name;
7. refuses to overwrite an existing database.

For Docker, local import folders must be mounted into the container. S3/R2
import does not require a mount. The import API is admin-only and accepts local
paths only inside the configured roots.

The GUI workflow and screenshots are documented in
[GUI console](gui.md#backup-workflow) and [Import](gui.md#import-workflow).

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
  ghcr.io/essmos/nonodb:0.22.0 \
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
  ghcr.io/essmos/nonodb:0.22.0 \
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
- Native backups have no incremental mode and copy the full compacted
  environment, optionally gzip-compressed. Incremental/deduplicated backups
  require the restic engine.
- `backup-all` is sequential, not an atomic cross-database snapshot.
- LMDB free pages remain in the live file until a compacting backup/restore.
- Snapshot retention does not verify files automatically.
- Cross-collection consistency is the consistency of the LMDB snapshot, not an
  application-level transaction boundary.
- A restored older backup also restores the TTL entries from that point in time;
  the next maintenance sweep removes documents whose expiry has passed.
