# Adaptive writer window

NonoDB uses one writer thread and group commit. Multiple writes queued at the
same time share one LMDB transaction and one commit. The writer chooses how long
to collect a batch before committing.

The window is now adaptive:

| Queue state | Window |
|---|---|
| no other writes queued | `NONODB_WRITER_WINDOW_IDLE_US` (default `100`) |
| small backlog | `NONODB_WRITER_WINDOW_US` (default `1000`) |
| 64+ waiting | `NONODB_WRITER_WINDOW_PRESSURE_US` (default `3000`) |
| 256+ waiting | `NONODB_WRITER_WINDOW_MAX_US` (default `5000`) |

The queue length includes the job the writer just received. The adaptive check
subtracts that job before deciding whether the writer is idle. Without that
adjustment the idle fast path is effectively unreachable.

## Configuration

```text
NONODB_WRITER_WINDOW_US=1000
NONODB_WRITER_WINDOW_PRESSURE_US=3000
NONODB_WRITER_WINDOW_MAX_US=5000
NONODB_WRITER_WINDOW_IDLE_US=100
```

All values are microseconds. The idle value is capped by the base value; the
maximum is at least the base value.

## Latency and throughput

A benchmark with 300 sequential durable writes on the release daemon:

| Configuration | p50 | p95 | ops/s |
|---|---:|---:|---:|
| old fixed 1 ms window | 1.809 ms | 2.784 ms | ~508 |
| adaptive, idle 100 µs | **0.812 ms** | 1.500 ms | ~1,056 |
| low latency, idle 10 µs + base 200 µs | **0.625 ms** | 1.188 ms | ~1,420 |
| base 2 ms, idle 100 µs | 0.768 ms | 1.346 ms | ~1,125 |

With one sequential client the queue is usually empty, so the writer uses the
idle window and responds quickly. Under concurrent load the queue is non-empty,
so it uses the batching window and amortizes commits across requests.

This changes latency only. It does not change fsync durability, atomicity, or
the single-writer model.

## Metrics

`/metrics` exposes the selected window and counters for each path:

```text
writer_window_us
writer_avg_window_us
writer_last_batch_size
writer_idle_windows_total
writer_base_windows_total
writer_pressure_windows_total
writer_max_windows_total
writer_fast_path_ratio
```

Interpretation:

- `writer_idle_windows_total` — no other writes were queued; fast path used.
- `writer_base_windows_total` — a small backlog used the normal window.
- `writer_pressure_windows_total` — 64+ writes were waiting; the pressure window
  was used for more batching.
- `writer_max_windows_total` — 256+ writes were waiting; the maximum window was
  used.
- `writer_fast_path_ratio` — fraction of writer batches chosen through the idle
  fast path.
- `writer_last_batch_size` and `writer_window_us` show the most recent decision;
  `writer_avg_window_us` is the cumulative average and is the better signal after
  a long run.

## Observed path selection

Example runs from the release daemon (`scripts/bench.py`, 24,000 writes and
96,000 reads, cache disabled). Host load varies, so compare shapes rather than
exact numbers.

| Run | Writes | Write p50 | Avg batch | Fast-path ratio | Pressure batches |
|---|---:|---:|---:|---:|---:|
| 12 clients, default | 3,331/s | 2.67 ms | 5.17 | 49.6% | 0 |
| 12 clients, base 5 ms | 1,617/s | 7.06 ms | 8.89 | 30.9% | 0 |
| 80 clients, default | 2,718/s | 19.10 ms | 16.15 | 27.1% | 27 |
| 80 clients, pressure 10 ms | 3,247/s | 16.87 ms | 18.38 | 19.8% | 18 |

Interpretation:

- With 12 clients the writer frequently sees an idle queue, so the fast path is
  used often. Raising the **base** window to 5 ms made batching larger but
  reduced throughput, because each batch waited longer without enough extra
  requests to amortize it.
- With 80 clients the queue is consistently busy. Raising only the **pressure**
  window to 10 ms increased batch size and write throughput; this is the useful
  tuning knob for sustained write-heavy traffic.
- `writer_fast_path_ratio` falling from ~50% (12 clients) to ~20% (80 clients)
  is expected: more clients means more requests are batched instead of using the
  idle fast path.
- Reads were measured in the same mixed runs and are shown here only to make the
  trade-off visible; they are not a pure read benchmark.

## Benchmark

Build the release daemon and run:

```bash
cargo build --release -p nonodb-daemon --bin nonodb-daemon
python3 scripts/writer_window_bench.py
```

The benchmark compares the old fixed-window behavior with adaptive idle values
and reports p50, p95, and aggregate throughput.

## Limitations

- The window is daemon-wide, not per database or collection.
- A single sequential writer cannot benefit from batching; the idle fast path is
  a deliberate latency optimization.
- Very high concurrency still trades latency for fewer fsyncs.
- On slow storage, raising the window may reduce fsync frequency but increase
  tail latency.
