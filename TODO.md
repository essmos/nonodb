# Implementation backlog

This file tracks remaining engineering work only. Completed implementation
history lives in [CHANGELOG.md](CHANGELOG.md); user-facing capabilities and
deliberate limitations are documented in [docs/roadmap.md](docs/roadmap.md).

## P1 — niski nakład, warto

- [ ] Add a graph traversal and Neo4j benchmark section to the project website /
      landing page, linking `docs/graph.md` and `docs/graph-benchmark.md`.

## P2 — stream follow-ups after v0.35.0

- [ ] Add a targeted replication test where a dead letter below
      `committed_seq` is retried and then the primary fails before replay; the
      retry is currently best-effort at-least-once.
- [ ] Add a direct replication assertion for `prune_stream` changing
      `truncated_below` on a group that already exists before the prune.
