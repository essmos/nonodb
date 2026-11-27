# Monitoring and alerting

NonoDB exposes two machine-readable views of the same process-local counters:

| Endpoint | Format | Authorization | Purpose |
|---|---|---|---|
| `GET /healthz` | JSON | public | Cheap liveness probe |
| `GET /readyz` | JSON | public | Writer + LMDB readiness probe |
| `GET /v1/capabilities` | JSON | public | Versioned feature and limit contract |
| `GET /metrics` | JSON | `metrics:read`, `admin:db`, or `*:*` | Human/debugging view and existing integrations |
| `GET /metrics/prometheus` | Prometheus text | `metrics:read`, `admin:db`, or `*:*` | Prometheus, Grafana Agent, VictoriaMetrics, OTel Collector |

Counters reset when the daemon restarts. Multi-database deployments expose
collection series as `database="<name>",collection="<name>"`.

## Metrics scope

Do not use an administrator token for scraping. Create a dedicated principal:

```bash
nonodb-cli auth principal add prometheus metrics:read
nonodb-cli auth token create prometheus > /etc/nonodb/prometheus-token
chmod 0640 /etc/nonodb/prometheus-token
chown root:prometheus /etc/nonodb/prometheus-token
```

`metrics:read` can read both metrics endpoints but cannot read collections,
manage databases, or change authorization. `admin:db` and `*:*` also imply
metrics access.

Prometheus needs a TCP listener because it does not scrape Unix sockets
directly. Keep the listener on loopback or behind a private network and TLS:

```bash
NONODB_TCP=127.0.0.1:7878
NONODB_AUTH_FILE=/etc/nonodb/auth.json
```

## Health and readiness

Liveness:

```bash
curl -fsS http://127.0.0.1:7878/healthz
```

Readiness:

```bash
curl -fsS http://127.0.0.1:7878/readyz
```

`/readyz` checks that the writer thread is still running and opens a real LMDB
read transaction. On replicas the writer is not required. It returns HTTP `503`
with per-check details when a required check fails. The Docker image uses
`nonodb-cli ready` as its `HEALTHCHECK`.

`/readyz/write` is the write-pool health check: `200` only on a writable
primary, otherwise `503` with `Location` set to the known primary URL. Use it
with `option httpchk GET /readyz/write` / `http-check expect status 200` so only
the current primary receives writes.

## Prometheus

The repository includes:

- [`deploy/prometheus.yml`](../deploy/prometheus.yml) — scrape configuration;
- [`deploy/prometheus-alerts.yml`](../deploy/prometheus-alerts.yml) — alert rules.

Start Prometheus with those files and set `credentials_file` to the token file
created above. The scrape endpoint is:

```text
http://127.0.0.1:7878/metrics/prometheus
```

Example output:

```text
# HELP nonodb_requests_total HTTP data and watch requests.
# TYPE nonodb_requests_total counter
nonodb_requests_total 1234
# HELP nonodb_queue_len Writer queue length.
# TYPE nonodb_queue_len gauge
nonodb_queue_len 0
nonodb_collection_requests_total{database="default",collection="orders"} 42
nonodb_request_duration_seconds_bucket{op="find",le="0.01"} 120
nonodb_request_duration_seconds_bucket{op="find",le="+Inf"} 140
nonodb_request_duration_seconds_sum{op="find"} 0.82
nonodb_request_duration_seconds_count{op="find"} 140
nonodb_disk_used_ratio{mount="data"} 0.41
nonodb_snapshot_age_seconds{database="default"} 120
```

Latency histograms cover the end-to-end `/v1/exec` request, including writer
queue time for writes. Long-poll and streaming watches are excluded because
their duration is client waiting time. Compute p95 latency with:

```promql
histogram_quantile(0.95, sum by (le, op) (rate(nonodb_request_duration_seconds_bucket[5m])))
```

Ordered point lookups also have dedicated counters and a histogram:

```text
nonodb_lookup_requests_total
nonodb_lookup_explains_total
nonodb_lookup_keys_total
nonodb_lookup_unique_keys_total
nonodb_lookup_found_total
nonodb_lookup_missing_total
nonodb_lookup_documents_read_total
nonodb_lookup_index_entries_read_total
nonodb_lookup_bytes_read_total
nonodb_lookup_bytes_returned_total
nonodb_lookup_errors_total
nonodb_lookup_duration_ns_total
nonodb_lookup_duration_seconds_bucket{op="lookup",le="0.001"}
nonodb_lookup_duration_seconds_bucket{op="explain_lookup",le="0.001"}
```

`nonodb_lookup_duration_seconds` measures the lookup execution itself, not the
HTTP round trip. Use it to watch index coverage and document-fetch rates:

```promql
histogram_quantile(0.95, sum by (le, op) (rate(nonodb_lookup_duration_seconds_bucket[5m])))
```

## OpenTelemetry

The daemon does not embed the OpenTelemetry SDK or push OTLP itself. This keeps
the single-binary footprint and avoids a second metrics pipeline. Use the
Collector's Prometheus receiver as the bridge:

```bash
otelcol --config deploy/otel-collector.yml
```

[`deploy/otel-collector.yml`](../deploy/otel-collector.yml) scrapes
`/metrics/prometheus` and batches metrics. Replace the `debug` exporter with
`otlphttp` or `otlp` for your backend. The same token file is used; the
Collector process needs read access to it.

This setup works with any OTLP-compatible backend, including Grafana Cloud,
Tempo/Mimir, Honeycomb, Datadog, and self-hosted OpenTelemetry Collectors.

## Included alerts

`deploy/prometheus-alerts.yml` covers:

- scrape target down;
- elevated request errors;
- writer queue backlog and writer saturation;
- data disk above 85% and 95%;
- stale snapshots and snapshot failures;
- elevated authentication failures and IP lockouts;
- per-collection error rate;
- replica lag, replication transport errors, apply failures, and stale replica acks;
- TLS certificates that expire within 14 days or have already expired.

Tune thresholds and `for:` durations for the deployment. The disk alerts use
`mount="data"`; snapshot alerts only fire when periodic snapshots are enabled
with `NONODB_SNAPSHOT_DIR`.

## Certificate metrics

When TLS is configured, the daemon exports the leaf certificate expiry as a
Unix timestamp:

| Metric | Meaning |
|---|---|
| `nonodb_tls_certificate_expiry_timestamp_seconds{kind}` | Expiry time of the configured certificate |

The `kind` label is `server` for `NONODB_TLS_CERT` and `replication` for
`NONODB_REPLICATION_CERT_FILE`. The series is absent when the corresponding
certificate is not configured. The same values are available in the `/metrics`
JSON response under `certificates`.

The included `NonoDBCertificateExpiring` alert fires when a certificate has
less than 14 days remaining:

```promql
nonodb_tls_certificate_expiry_timestamp_seconds - time() < 14 * 24 * 3600
```

Rotate the certificate and trigger the normal TLS reload by replacing the file.
For a replication client certificate, restart the daemon so the outbound HTTP
client picks up the new identity.

## Disk and snapshot metrics

Filesystem capacity is sampled from `NONODB_DATA` and, when configured,
`NONODB_SNAPSHOT_DIR`:

| Metric | Meaning |
|---|---|
| `nonodb_disk_total_bytes{mount}` | Filesystem capacity |
| `nonodb_disk_free_bytes{mount}` | Free bytes |
| `nonodb_disk_used_bytes{mount}` | Used bytes |
| `nonodb_disk_used_ratio{mount}` | Used ratio, `0.0`–`1.0` |

Periodic snapshots expose per-database series:

| Metric | Meaning |
|---|---|
| `nonodb_snapshot_success_total` | Successful snapshots |
| `nonodb_snapshot_failures_total` | Failed snapshots |
| `nonodb_snapshot_last_success_timestamp_seconds` | Unix time of the last success |
| `nonodb_snapshot_age_seconds` | Age of the last success |
| `nonodb_snapshot_last_failure_timestamp_seconds` | Unix time of the last failure |
| `nonodb_snapshot_last_duration_ms` | Duration of the last successful snapshot |

With PITR enabled, WAL retention exposes per-database series:

| Metric | Meaning |
|---|---|
| `nonodb_wal_bytes` | Bytes used by retained WAL |
| `nonodb_wal_oldest_age_seconds` | Age of the oldest retained WAL segment |
| `nonodb_wal_last_sequence` | Highest sequence archived in the WAL |
| `nonodb_wal_full` | `1` while the hard limit rejects writes |
| `nonodb_pitr_available` | `1` when a snapshot plus WAL can serve a restore |
| `nonodb_wal_pruned_bytes_total` | WAL bytes removed by covered-snapshot retention |

`nonodb-cli backup` runs outside the daemon, so its age is not reported here.
For external backups, write a timestamp marker in your backup script and alert
on the marker's age, or use the target's native backup monitoring.

## Replication metrics

Replication-log metrics are exposed when the daemon has any known databases.
They are cheap gauges read from LMDB; they do not scan documents.

| Metric | Meaning |
|---|---|
| `nonodb_replication_enabled` | `1` when durable writer capture is enabled |
| `nonodb_replication_role{role}` | Current persisted role |
| `nonodb_replication_epoch` | Current role epoch |
| `nonodb_replication_origin_info{origin}` | Configured node identity |
| `nonodb_replication_head{database}` | Highest allocated replication sequence |
| `nonodb_replication_applied{database,origin}` | Highest applied source sequence |
| `nonodb_replication_applied_epoch{database,origin}` | Highest applied source role epoch |
| `nonodb_replication_lag{database,origin}` | Source head minus applied sequence |
| `nonodb_replication_applied_total` | Transactions applied successfully |
| `nonodb_replication_transport_errors_total` | Pull transport, HTTP, or decode errors |
| `nonodb_replication_apply_errors_total` | Failed replica applies |
| `nonodb_replication_last_success_unix` | Unix time of the last successful pull |
| `nonodb_replication_retention_runs_total` | Retention passes with replica acks |
| `nonodb_replication_pruned_total` | Log entries removed by automatic retention |
| `nonodb_replication_min_ack{database,origin}` | Minimum acknowledged replica sequence |
| `nonodb_replication_ack_nodes{database,origin}` | Number of replicas that acknowledged the source |
| `nonodb_replication_ack_seq{database,origin,node}` | Sequence acknowledged by one replica |
| `nonodb_replication_ack_age_seconds{database,origin,node}` | Time since the last replica acknowledgement |

`nonodb_replication_head` is the local log head. On a replica,
`nonodb_replication_applied` and `nonodb_replication_lag` track the configured
source. Apply also invalidates the replica query cache so reads do not serve
stale documents.

## Time-series metrics

Time-series maintenance publishes gauges after each retention sweep and
counters for retention/compaction work. Gauges are updated when automatic
retention is enabled (`NONODB_TS_RETENTION_INTERVAL_SECS` > 0).

| Metric | Meaning |
|---|---|
| `nonodb_ts_collections` | Time-series collections seen by the sweep |
| `nonodb_ts_rows` | Live rows across those collections |
| `nonodb_ts_deleted_rows` | Deleted rows still encoded in bucket bitmaps |
| `nonodb_ts_buckets` | Buckets across all time-series collections |
| `nonodb_ts_encoded_bytes` | Encoded bucket bytes |
| `nonodb_ts_raw_bytes` | Raw JSON bytes represented by those buckets |
| `nonodb_ts_retention_runs_total` | Automatic retention passes |
| `nonodb_ts_retention_dropped_buckets_total` | Whole buckets removed by retention |
| `nonodb_ts_retention_boundary_buckets_total` | Boundary buckets marked by retention |
| `nonodb_ts_retention_errors_total` | Failed retention passes |
| `nonodb_ts_compaction_requests_total` | Compaction requests written |
| `nonodb_ts_compactions_total` | Compactions completed (startup or online) |
| `nonodb_ts_compacted_bytes_total` | Bytes reclaimed by compaction |

A rising `nonodb_ts_deleted_rows` with a flat `nonodb_ts_encoded_bytes` means
bitmap deletes are accumulating; check `NONODB_TS_COMPACT_THRESHOLD_MB`. The
daemon's online compactor picks up the marker and swaps the file once the
database has no active runtime (`NONODB_TS_ONLINE_COMPACT_INTERVAL_SECS`).

## Stream metrics

Streams publish per-group gauges on every `/metrics/prometheus` scrape and
process-wide counters. Gauges are read from LMDB (head, committed offset, lease
rows, dead letters); they do not scan documents.

| Metric | Meaning |
|---|---|
| `nonodb_stream_committed_seq{database,stream,group}` | Group committed offset |
| `nonodb_stream_lag_events{database,stream,group}` | Head minus committed offset |
| `nonodb_stream_inflight_events{database,stream,group}` | Events leased but not committed |
| `nonodb_stream_dead_events{database,stream,group}` | Dead-lettered events |
| `nonodb_stream_claims_total` | Batches claimed by a member |
| `nonodb_stream_redeliveries_total` | Batches delivered more than once |
| `nonodb_stream_nacks_total` | Batches released with nack |
| `nonodb_stream_dead_letters_total` | Events dead-lettered after `max_attempts` |
| `nonodb_stream_truncated_total` | Events dropped by offset retention |

How to read them:

- A flat `nonodb_stream_lag_events` with a growing `nonodb_stream_inflight_events`
  means members hold leases without committing: check for crashed workers before
  `max_attempts` dead-letters the batch.
- A rising `nonodb_stream_redeliveries_total` with a stable lag indicates
  timeouts that are too short for the handler, not missing events.
- `nonodb_stream_lag_events > 0` on every group keeps retention from advancing
  (`min(head - max_events, age boundary, slowest committed - min_seq_keep)`), so
  disk use follows the slowest consumer. A paused consumer is a disk-growth
  decision.
- `nonodb_stream_truncated_total` only moves when a stream has retention
  configured (`max_events` or `max_age_ms`). A group behind the window fails with
  `stream_truncated` instead of skipping events.

## Recommended alerts

At minimum, alert on:

1. `up{job="nonodb"} == 0`
2. `nonodb_disk_used_ratio{mount="data"} > 0.85`
3. `nonodb_snapshot_age_seconds > 3600` when snapshots are enabled
4. sustained `nonodb_queue_len` growth
5. a sustained `nonodb_errors_total` rate
6. `nonodb_auth_failures_total` and `nonodb_auth_blocks_total`
7. `nonodb_tls_certificate_expiry_timestamp_seconds - time() < 14 * 24 * 3600`
8. `nonodb_wal_full == 1`
9. `nonodb_pitr_available == 0` when PITR is enabled and the WAL has data
10. `nonodb_wal_oldest_age_seconds` above the configured PITR window
11. `nonodb_tls_revocation_refresh_errors_total` increasing when CRL/OCSP is configured
12. `nonodb_tls_revocation_unavailable_total` increasing with fail-closed mTLS
13. `nonodb_stream_lag_events > <budget>` for more than one scrape interval
14. `nonodb_stream_dead_events` increasing on a group with `max_attempts` set
15. `nonodb_stream_inflight_events` flat at a non-zero value while lag stays flat

`GET /metrics` remains available for existing JSON consumers and includes the
same extra `disk`, `snapshots`, `wal`, and `collection_metrics` fields.
