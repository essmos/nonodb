# Adaptive writer and deferred indexes

This branch adds two experimental mechanisms:

1. an adaptive group-commit window,
2. an opt-in worker for deferred non-unique secondary indexes.

Both preserve the single-writer LMDB model. The HTTP/JSON API is unchanged.

## Adaptive group-commit window

The writer still batches requests into one LMDB transaction. The collection
window now depends on real queue pressure:

| Queue state | Window |
|---|---|
| empty after removing the current job | `NONODB_WRITER_WINDOW_IDLE_US` (default `100`) |
| normal backlog | `NONODB_WRITER_WINDOW_US` (default `1000`) |
| 64+ waiting | up to 3 ms |
| 256+ waiting | `NONODB_WRITER_WINDOW_MAX_US` (default `5000`) |

The important bug fix is that `QUEUE_LEN` includes the job the writer just
received. The adaptive check subtracts that job before deciding whether the
writer is idle.

Configuration:

```text
NONODB_WRITER_WINDOW_US=1000
NONODB_WRITER_WINDOW_MAX_US=5000
NONODB_WRITER_WINDOW_IDLE_US=100
```

A benchmark with 300 sequential durable writes on the release daemon:

| Configuration | p50 | p95 | ops/s |
|---|---:|---:|---:|
| old fixed 1 ms idle window | 1.809 ms | 2.784 ms | ~508 |
| adaptive, idle 100 µs | 0.812 ms | 1.500 ms | ~1,056 |
| low latency, idle 10 µs + base 200 µs | 0.625 ms | 1.188 ms | ~1,420 |

Under concurrent load the queue is non-empty, so the writer uses the batching
window again. This is a latency/throughput trade-off, not an fsync change.

`/metrics` exposes:

```text
writer_window_us
```

## Deferred non-unique indexes

Enable with:

```bash
NONODB_ASYNC_INDEXES=1 nonodb-daemon
```

Disable the worker for tests or maintenance:

```bash
NONODB_INDEX_WORKER=0 nonodb-daemon
```

### What is deferred

Only **non-unique secondary indexes, including geo** are deferred.

These remain synchronous and immediately consistent:

- unique indexes,
- text indexes,
- fuzzy indexes,
- vector indexes,
- document writes, history, TTL, and events.

### How it works

For a write that changes a non-unique index:

1. The document and a durable queue entry are committed in the same LMDB
   transaction.
2. The queue entry contains the collection, document id, and old/new document.
3. The HTTP write returns after the document commit.
4. A background thread sends queued changes through the normal single-writer
   channel in batches.
5. The worker applies index entries and deletes the queue entries atomically.

There is no second writer and no cross-process LMDB access.

### Read correctness

While the durable queue is non-empty, non-unique secondary/geo/sort index plans
are disabled for that database. Queries fall back to collection scans, so they
still return correct read-after-write results. Unique indexes continue to be
used because they are maintained synchronously.

Once the worker drains the queue, normal index planning resumes.

This means the mode trades:

- lower document-write work,
- potentially more scans while the worker is behind,
- additional queue write amplification (old/new documents),
- more total work to fully update the indexes.

### Crash and restart

Queue entries and the pending counter are part of the LMDB environment. A crash
after the document commit cannot lose an index change. On startup the worker
finds the durable queue and drains it. Until it does, queries use scans.

### Metrics

```text
index_queue_pending
index_queue_processed_total
```

### Test

```bash
cargo build -p nonodb-daemon
python3 scripts/async_index_test.py
```

The test disables the worker, verifies scan fallback and synchronous unique
constraints, restarts the daemon, waits for the queue to drain, and verifies that
the index becomes usable.

### Benchmark

10,000-document batch with three non-unique indexes:

| Mode | Document write | Drain complete |
|---|---:|---:|
| synchronous indexes | ~55,800 docs/s | ~0.179 s |
| async indexes | ~56,800 docs/s | ~0.341 s |

The raw write is only marginally faster, while complete index processing takes
longer because every changed document is queued with old/new versions. Async
indexes are therefore useful only when index maintenance is substantially more
expensive than queueing, or when queries can tolerate scans during lag. They are
not enabled by default.

## Limitations

- No per-collection opt-in yet; the switch is daemon-wide.
- Queue entries store full old/new documents, causing write amplification.
- Text/fuzzy/vector indexes remain synchronous.
- Queries may scan while the worker is behind.
- The worker still uses the single writer for index mutations.
