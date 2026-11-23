# Implementation backlog

This file tracks remaining engineering work only. Completed implementation
history lives in [CHANGELOG.md](CHANGELOG.md); user-facing capabilities and
deliberate limitations are documented in [docs/roadmap.md](docs/roadmap.md).

## P0 — konieczne przed produkcją

- [ ] Real sparse/partial index semantics, including unique-index behavior for
      missing fields, query planning, migration notes, and MongoDB compatibility
      tests.
- [ ] Per-database quotas and usage accounting for data, indexes, history, and
      sidecars, with operator-visible warnings and write rejection policy.
- [ ] Safe restore-in-place API and GUI flow with typed confirmation, preflight
      verification, audit logging, and rollback guidance.

## P1 — niski nakład, warto

- [ ] Publish `nonodb-client` to PyPI using the prepared manual workflow.
- [ ] Client-side `lookup_join` helpers in Python, Node.js, Rust, and Go,
      preserving order and missing-key alignment.
- [ ] Stored BSON regex values with matching `$type` entries and documented
      index behavior.
