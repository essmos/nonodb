# Columnar release sign-off record

This record is the signed checklist required by `AC-XCUT-009` in
[columnar collections acceptance criteria](columnar-collections-acceptance.md).
It is intentionally separate from the acceptance table so that a signature is
an explicit act, not a self-asserted checkbox.

Branch: `experiment/ts-general` (fork `rod-rgba/nonodb`)

| Role | Name | Signature state | Evidence reviewed |
|---|---|---|---|
| Implementer | `rod-rgba` | signed | `cargo test --workspace -j1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, columnar integration suites, `docs/columnar-performance.md` |
| Operator / benchmark owner | `rod-rgba` | signed | canonical benchmark gates, 10M-row soak, release and debug binaries, backup/restore and replication drills |
| Independent completion auditor | completion-audit worker | recorded by audit verdict | independent branch extraction, differential probes, acceptance evidence review |

The independent reviewer entry is completed by the completion-audit verdict
for this goal. It is deliberately not pre-signed by the implementer; the audit
result is the reviewer's signature event. If the audit rejects the branch, this
record is not a release approval.

## Checklist

- [x] All `AC-CORE` through `AC-XCUT` rows are `[x]`.
- [x] The compatibility matrix in the plan has no unreleased gaps.
- [x] The differential suite is part of normal CI.
- [x] Storage claims include ids, id map, indexes, and history.
- [x] Backup/restore, replication/failover, branch, and crash drills pass.
- [x] Linux, macOS, and Windows CI legs run columnar core tests; Linux also
      runs MongoDB conformance once with standard and once with columnar
      collections.
- [x] Docs, clients, CLI, GUI, and MongoDB wire are updated.
- [x] Existing collections are not converted automatically.

## Reproducible gates

```bash
cargo test --workspace -j1
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
bash scripts/clients_test.sh
NONODB_CONFORMANCE_PROFILE=columnar python3 scripts/mongo_conformance_test.py
NONODB_OLD_DAEMON=/path/to/v0.34.0/nonodb-daemon \
  bash scripts/columnar_old_binary_test.sh
```

The old-binary gate downloads the public `v0.34.0` release when run in CI and
checks both direct open and backup/restore rejection without modifying the
database.
