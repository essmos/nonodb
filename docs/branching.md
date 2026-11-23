# Branching a database

NonoDB can create an offline branch of a database for CI, integration tests,
migration rehearsals, or experiments. A branch is a complete, independent
database with its own `data.mdb`, writer, lock file, indexes, TTL/history
metadata, and search/vector sidecars copied or rebuilt from the data.

```bash
# Stop the daemon first when cloning a live database directory.
systemctl stop nonodb
nonodb-cli --db app1 branch app1-ci --data /var/lib/nonodb --verify
systemctl start nonodb
```

The target is a new directory under the data root:

```text
/var/lib/nonodb/
├── app1/
│   ├── data.mdb
│   ├── lock.mdb
│   └── search/
└── app1-ci/
    ├── data.mdb
    ├── lock.mdb
    └── search/
```

The source database is selected with `--db` or `NONODB_DB`. The target name is
the positional argument. `--data` may be omitted when `NONODB_DATA` is set.

## Branch from a snapshot

A snapshot is already consistent, so it is the safest source while the daemon
is running or when the live database must not be touched:

```bash
nonodb-cli branch app1-ci \
  --from-snapshot /var/backups/nonodb/snapshots/app1/data-1767225600000.mdb \
  --data /var/lib/nonodb \
  --verify
```

`--from-snapshot` accepts a `data-<unix-ms>.mdb` file, a `data.mdb` file, or a
directory containing `data.mdb` or `data-<unix-ms>.mdb` snapshots (the newest
snapshot is selected). Sidecars are looked up next to that file or inside that
directory, so a snapshot root that contains `search/` and `vector-ann/`
preserves them.

Daemon snapshots normally contain only `data-<unix-ms>.mdb`; the search and
vector-ann sidecar directories stay with the live database. The branch command
therefore rebuilds missing text, fuzzy, and enabled ANN sidecars inside the
staging clone before publishing it. The default snapshot CI flow below produces
a clone that can answer text, fuzzy, and vector queries immediately.

## Copy-on-write

On Linux the command tries `FICLONE` (reflink); on macOS it tries `clonefile`.
On a CoW filesystem this makes the branch metadata-only and effectively instant
even for a large database:

```bash
nonodb-cli --db app1 branch app1-ci --data /var/lib/nonodb --json
```

```json
{
  "target": "app1-ci",
  "method": "reflink",
  "bytes": 734003200,
  "elapsed_ms": 12,
  "sidecars": ["search"],
  "sidecar_rebuilds": 0,
  "verification": null
}
```

If the filesystem or kernel does not support reflinking, the command falls back
to a normal byte copy and reports `"method": "copy"`, plus a `"warning"` field.
`bytes` is the final size of the clone (data plus sidecars), and
`sidecar_rebuilds` reports how many missing search/fuzzy/ANN sidecars were built
during the clone.
The human-readable output prints the warning on stderr. The result is correct,
but not instant. `--verify` adds a deep LMDB/index check; it does not change the
copy method.

## Options and safety

| Option | Meaning |
|---|---|
| `--data ROOT` | Instance data root; defaults to `NONODB_DATA` |
| `--from-snapshot PATH` | Branch from a snapshot instead of the selected live database |
| `--force` | Replace an existing target only after the new branch is complete |
| `--verify` | Deep-verify the branch before publishing it |
| `--no-sidecars` | Skip copying and rebuilding `search/` and `vector-ann/` |
| `--json` | Machine-readable method, size, elapsed time, sidecars, verification |

The source is never modified. `lock.mdb` is not copied; the daemon creates a
new lock file for the branch. `--force` swaps the old target out only after the
temporary branch has been created and verified, so a failed clone leaves the
existing target intact. Auth, TLS, backup configuration, audit logs, and other
instance-wide files are not copied.

`--no-sidecars` is an explicit opt-out. A clone made with it has no search,
fuzzy, or ANN sidecars, so those queries fail until you restart the daemon with
`NONODB_REBUILD_MISSING_SEARCH=1` (or rebuild the branch without the flag). Do
not use `--no-sidecars` for a CI branch that runs text, fuzzy, or vector tests.

## CI example

Create a disposable branch from a known snapshot before a test run:

```bash
BRANCH="ci-$CI_JOB_ID"
nonodb-cli branch "$BRANCH" \
  --from-snapshot "$SNAPSHOT" \
  --data "$NONODB_DATA" \
  --force

export NONODB_DB="$BRANCH"
pytest
```

Remove the branch directory after the job to discard all writes. There is no
merge/rebase between branches; use a new branch for each isolated test run.

## Limits

- Offline only. There is no daemon API for branching a database while its
  writer is active; use a snapshot for a live database.
- Branching a stopped database directory is a filesystem copy, not a logical
  dump. Do not branch a directory while another process is writing it.
- Sidecars are derived data. A normal branch copies them and rebuilds any that
  are missing; `--no-sidecars` deliberately leaves them absent. The daemon does
  not rebuild them on first query, only at startup when
  `NONODB_REBUILD_MISSING_SEARCH=1` is set.
- Reflink performance depends on the filesystem. XFS, Btrfs, and ZFS are common
  choices; ext4 and some overlay filesystems usually fall back to a full copy.
- A branch has no merge/diff/rebase support and is not registered in a cluster
  automatically. The daemon discovers it as a normal database directory.
