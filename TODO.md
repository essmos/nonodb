# Implementation backlog

This file tracks remaining engineering work only. Completed implementation
history lives in [CHANGELOG.md](CHANGELOG.md); user-facing capabilities and
deliberate limitations are documented in [docs/roadmap.md](docs/roadmap.md).

## P1 — niski nakład, warto

- [ ] Publish `nonodb-client` to PyPI using the prepared manual workflow.
- [ ] Client-side `lookup_join` helpers in Python, Node.js, Rust, and Go,
      preserving order and missing-key alignment.
- [ ] Stored BSON regex values with matching `$type` entries and documented
      index behavior.
