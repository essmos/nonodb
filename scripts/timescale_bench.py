#!/usr/bin/env python3
"""Compare the NonoDB time-series workload shape with TimescaleDB.

The dataset and operations mirror ``nonodb-core/examples/timeseries_bench.rs``:
100k rows, 10 series, one sample per second, 8 columns. TimescaleDB runs in a
local container; point/update/delete latency is measured one statement at a
time, range/count/aggregate latency includes fetching the result.

Usage:
  python3 scripts/timescale_bench.py --dsn "host=127.0.0.1 port=55432 dbname=bench user=postgres password=bench"

Requires ``psycopg2-binary``. The script recreates the ``metrics`` table.
"""

import argparse
import statistics
import time
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values

ROWS = 100_000
SERIES = 10
BASE_MS = 1_700_000_000_000
BASE = datetime.fromtimestamp(BASE_MS / 1000, tz=timezone.utc)
STATUS = ["ok", "warn", "error", "offline"]


def doc(idx: int):
    ts = BASE + timedelta(seconds=idx // SERIES)
    return (
        ts,
        f"sensor-{idx % SERIES}",
        21.0 + (idx % 97) * 0.01,
        40.0 + (idx % 53) * 0.05,
        1013.0 + (idx % 31) * 0.1,
        STATUS[idx % 4],
        idx % 8,
        not idx % 3 == 0,
    )


def timed(fn, n: int):
    samples = []
    for _ in range(n):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000)
    samples.sort()
    p50 = statistics.median(samples)
    p95 = samples[min(len(samples) - 1, int(len(samples) * 0.95))]
    p99 = samples[min(len(samples) - 1, int(len(samples) * 0.99))]
    return p50, p95, p99


def report(label, fn, n):
    p50, p95, p99 = timed(fn, n)
    print(f"{label:<42} {n:>7} ops  p50 {p50:8.3f} ms  p95 {p95:8.3f} ms  p99 {p99:8.3f} ms")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default="host=127.0.0.1 port=55432 dbname=bench user=postgres password=bench")
    parser.add_argument("--row-inserts", type=int, default=5000)
    parser.add_argument("--reads", type=int, default=1000)
    args = parser.parse_args()

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
    cur.execute("DROP TABLE IF EXISTS metrics CASCADE")
    cur.execute(
        """
        CREATE TABLE metrics (
            id bigserial,
            ts timestamptz NOT NULL,
            device text NOT NULL,
            temperature double precision,
            humidity double precision,
            pressure double precision,
            status text,
            level integer,
            ok boolean
        )
        """
    )
    cur.execute(
        "SELECT create_hypertable('metrics','ts', chunk_time_interval => INTERVAL '1 hour')"
    )

    # Per-row inserts (same shape as the NonoDB insert benchmark).
    row_count = min(args.row_inserts, ROWS)
    insert_latency = []
    for idx in range(row_count):
        start = time.perf_counter()
        cur.execute(
            "INSERT INTO metrics (ts,device,temperature,humidity,pressure,status,level,ok) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            doc(idx),
        )
        insert_latency.append((time.perf_counter() - start) * 1000)
    insert_latency.sort()
    print(
        f"{'insert one row':<42} {row_count:>7} ops  p50 {statistics.median(insert_latency):8.3f} ms  "
        f"p99 {insert_latency[min(len(insert_latency)-1, int(len(insert_latency)*0.99))]:8.3f} ms"
    )

    # Bulk load the rest so the read benchmark has 100k rows.
    bulk_start = time.perf_counter()
    batch = []
    for idx in range(row_count, ROWS):
        batch.append(doc(idx))
        if len(batch) == 1000:
            execute_values(
                cur,
                "INSERT INTO metrics (ts,device,temperature,humidity,pressure,status,level,ok) VALUES %s",
                batch,
            )
            batch.clear()
    if batch:
        execute_values(
            cur,
            "INSERT INTO metrics (ts,device,temperature,humidity,pressure,status,level,ok) VALUES %s",
            batch,
        )
    bulk_seconds = time.perf_counter() - bulk_start
    print(f"{'bulk load (1000-row batches)':<42} {ROWS-row_count:>7} rows {ROWS-row_count/bulk_seconds:9.0f} rows/s")

    # Index used by the 1h series range query.
    index_start = time.perf_counter()
    cur.execute("CREATE INDEX metrics_device_ts ON metrics (device, ts)")
    print(f"{'create index (device, ts)':<42} {time.perf_counter()-index_start:8.3f} s")

    index_start = time.perf_counter()
    cur.execute("CREATE INDEX metrics_id ON metrics (id)")
    print(f"{'create index (id)':<42} {time.perf_counter()-index_start:8.3f} s")

    cur.execute("SELECT id FROM metrics ORDER BY id LIMIT 1")
    min_id = cur.fetchone()[0]
    cur.execute("SELECT max(id) FROM metrics")
    max_id = cur.fetchone()[0]
    span = max_id - min_id
    point_ids = [min_id + (i * 7919) % span for i in range(args.reads)]

    def report_samples(label, samples):
        samples.sort()
        p50 = statistics.median(samples)
        p95 = samples[min(len(samples) - 1, int(len(samples) * 0.95))]
        p99 = samples[min(len(samples) - 1, int(len(samples) * 0.99))]
        print(f"{label:<42} {len(samples):>7} ops  p50 {p50:8.3f} ms  p95 {p95:8.3f} ms  p99 {p99:8.3f} ms")

    point_latency = []
    for pid in point_ids:
        start = time.perf_counter()
        cur.execute("SELECT * FROM metrics WHERE id = %s", (pid,))
        cur.fetchone()
        point_latency.append((time.perf_counter() - start) * 1000)
    report_samples("find by id (point)", point_latency)

    wide_start = BASE + timedelta(hours=1)
    wide_end = wide_start + timedelta(hours=1)
    narrow_end = wide_start + timedelta(minutes=1)

    def range_query(device, start, end):
        cur.execute(
            "SELECT * FROM metrics WHERE device=%s AND ts >= %s AND ts < %s",
            (device, start, end),
        )
        return cur.fetchall()

    report("find 1 min, 1 series", lambda: range_query("sensor-0", wide_start, narrow_end), 200)
    report("find 1 h, 1 series", lambda: range_query("sensor-0", wide_start, wide_end), 50)
    report(
        "count 1 h, 1 series",
        lambda: (cur.execute("SELECT count(*) FROM metrics WHERE device=%s AND ts >= %s AND ts < %s", ("sensor-0", wide_start, wide_end)), cur.fetchone()),
        50,
    )
    report(
        "aggregate $group 1 h",
        lambda: (
            cur.execute(
                "SELECT status, count(*), avg(temperature) FROM metrics WHERE device=%s AND ts >= %s AND ts < %s GROUP BY status ORDER BY 2 DESC",
                ("sensor-0", wide_start, wide_end),
            ),
            cur.fetchall(),
        ),
        20,
    )

    update_ids = [min_id + (i * 131) % span for i in range(100)]
    update_latency = []
    for ident in update_ids:
        start = time.perf_counter()
        cur.execute("UPDATE metrics SET level=9 WHERE id=%s", (ident,))
        update_latency.append((time.perf_counter() - start) * 1000)
    update_latency.sort()
    print(f"{'update by primary key':<42} {len(update_latency):>7} ops  p50 {statistics.median(update_latency):8.3f} ms  p99 {update_latency[-1]:8.3f} ms")

    delete_ids = [min_id + (i * 977) % span for i in range(50)]
    delete_latency = []
    for ident in delete_ids:
        start = time.perf_counter()
        cur.execute("DELETE FROM metrics WHERE id=%s", (ident,))
        delete_latency.append((time.perf_counter() - start) * 1000)
    delete_latency.sort()
    print(f"{'delete by primary key':<42} {len(delete_latency):>7} ops  p50 {statistics.median(delete_latency):8.3f} ms  p99 {delete_latency[-1]:8.3f} ms")

    index_start = time.perf_counter()
    cur.execute("CREATE INDEX metrics_status_device ON metrics (status, device)")
    print(f"{'create index (status, device)':<42} {time.perf_counter()-index_start:8.3f} s")
    report(
        "find indexed status+series, limit 100",
        lambda: (cur.execute("SELECT * FROM metrics WHERE status='error' AND device='sensor-0' LIMIT 100"), cur.fetchall()),
        200,
    )

    cur.execute("SELECT hypertable_size('metrics'), pg_total_relation_size('metrics')")
    hyper, total = cur.fetchone()
    print(f"storage uncompressed: hypertable {hyper/1048576:.2f} MB, total {total/1048576:.2f} MB")

    cur.execute("ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby='ts DESC')")
    cur.execute("SELECT compress_chunk(c) FROM show_chunks('metrics') c")
    cur.execute("SELECT hypertable_size('metrics'), pg_total_relation_size('metrics')")
    hyper, total = cur.fetchone()
    print(f"storage compressed:   hypertable {hyper/1048576:.2f} MB, total {total/1048576:.2f} MB")
    cur.execute("SELECT count(*) FROM metrics")
    print(f"rows after compression: {cur.fetchone()[0]}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
