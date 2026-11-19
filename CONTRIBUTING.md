# Contributing to nonodb

Thanks for taking the time to improve nonodb. This project is a documented
MongoDB-compatible subset, not a drop-in replacement for every MongoDB feature.
Changes should preserve the single-writer model, LMDB durability, and the rule
that unsupported behavior fails loudly instead of silently doing the wrong
thing.

## Before you start

- Read [docs/architecture.md](docs/architecture.md) and
  [docs/roadmap.md](docs/roadmap.md) to understand the design and deliberate
  limits.
- For a bug, include a minimal reproduction and the affected version.
- For a larger feature, open an issue first so the API and storage impact can be
  discussed before implementation.
- Do not include production data, tokens, certificates, private keys, vault
  material, or document bodies in issues or pull requests.

## Development setup

Prerequisites are listed in [docs/development.md](docs/development.md). A quick
Linux/macOS setup is:

```bash
git clone https://github.com/essmos/nonodb.git
cd nonodb
cargo build
```

For the Python client and scripts:

```bash
python3 -m pip install httpx
```

For the embedded GUI or Node.js client, use Node.js 22 or newer. The Go client
uses Go 1.22 or newer.

## Required checks

Run these before opening a pull request:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

For changes that affect search, indexing, replication, authentication, backup,
or the MongoDB wire protocol, also run the relevant scripts under `scripts/`.
Examples:

```bash
NONODB_SOCK=/tmp/nonodb.sock python3 scripts/search_test.py
NONODB_SOCK=/tmp/nonodb.sock python3 scripts/search_variants_test.py
python3 scripts/security_check.py
```

Use a temporary data directory and temporary credentials. Never run tests against
a production database.

## Code guidelines

- Keep storage and query semantics in `nonodb-core`; the daemon should handle
  transport, authorization, metrics, and request validation.
- Maintain all indexes in the same LMDB transaction as the document write.
- Use an index only when equivalence is provable; otherwise fall back to a scan
  and residual matcher.
- Reject unsupported options and MongoDB commands with a clear error. Never
  accept an option and silently ignore it.
- Preserve existing request and response shapes unless the change is explicitly
  a breaking change.
- Add unit tests for core semantics and an end-to-end script for HTTP, client,
  CLI, or MongoDB wire behavior.
- Update the relevant file in `docs/` when behavior, configuration, limits, or
  operations change.
- Keep release artifacts out of local commits. Release builds are produced only
  by GitHub Actions.

## Commits and pull requests

- Keep commits focused and use a clear subject in the imperative mood.
- Describe the user-visible behavior, tests run, and any migration or reindex
  requirement in the pull request.
- Mention deliberate limitations and follow-up work.
- Do not move or retag published releases.
- Do not commit generated build output, local databases, tokens, or secret
  material.

## Security reports

Do not open a public issue for a suspected vulnerability. Follow
[SECURITY.md](SECURITY.md).
