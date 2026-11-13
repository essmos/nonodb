# Adaptive writer window

NonoDB uses one writer thread and group commit. Multiple writes queued at the
same time share one LMDB transaction and one commit. The writer chooses how long
to collect a batch before committing.

The window is now adaptive:

| Queue state | Window |
|---|---|
| no other writes queued | `NONODB_WRITER_WINDOW_IDLE_US` (default `100`) |
| small backlog | `NONODB_WRITER_WINDOW_US` (default `1000`) |
| 64+ waiting | up to 3 ms |
| 256+ waiting | `NONODB_WRITER_WINDOW_MAX_US` (default `5000`) |

The queue length includes the job the writer just received. The adaptive check
subtracts that job before deciding whether the writer is idle. Without that
adjustment the idle fast path is effectively unreachable.

## Configuration

```text
NONODB_WRITER_WINDOW_US=1000
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

`/metrics` exposes:

```text
writer_window_us
```

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
