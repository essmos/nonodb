# Implementation backlog

This file tracks remaining engineering work only. Completed implementation
history lives in [CHANGELOG.md](CHANGELOG.md); user-facing capabilities and
deliberate limitations are documented in [docs/roadmap.md](docs/roadmap.md).

## P1 — lookup enhancements

- [ ] Add `explain_lookup` with selected index, covering status, key counts,
      found/missing counts, documents read, bytes returned, and elapsed time.
      Add aggregate lookup metrics and latency histograms.
- [ ] Add client-side `lookup_chunked` helpers that split large key lists into
      bounded requests while preserving input order and missing-key alignment.
- [ ] Validate lookup limits consistently on missing-collection and batch-read
      paths, matching the normal `lookup` validation.

## P1 — durability and operations

- [ ] Encryption-at-rest integration guidance.

## P2 — GUI and operator experience

- [ ] Create/drop databases directly in the GUI.
- [ ] Simultaneous multi-daemon connection tabs.
- [ ] Accessibility audit and expanded keyboard navigation.
- [ ] Export/import of saved views and filters without credentials.

## P2 — clients

- [ ] Published Python package.
