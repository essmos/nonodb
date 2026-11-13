# Monitoring and alerting

NonoDB exposes two machine-readable views of the same process-local counters:

| Endpoint | Format | Authorization | Purpose |
|---|---|---|---|
| `GET /healthz` | JSON | public | Cheap liveness probe |
| `GET /readyz` | JSON | public | Writer + LMDB readiness probe |
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
read transaction. It returns HTTP `503` with per-check details when either check
fails. The Docker image uses `nonodb-cli ready` as its `HEALTHCHECK`.

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
nonodb_disk_used_ratio{mount="data"} 0.41
nonodb_snapshot_age_seconds{database="default"} 120
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
- per-collection error rate.

Tune thresholds and `for:` durations for the deployment. The disk alerts use
`mount="data"`; snapshot alerts only fire when periodic snapshots are enabled
with `NONODB_SNAPSHOT_DIR`.

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

`nonodb-cli backup` runs outside the daemon, so its age is not reported here.
For external backups, write a timestamp marker in your backup script and alert
on the marker's age, or use the target's native backup monitoring.

## Recommended alerts

At minimum, alert on:

1. `up{job="nonodb"} == 0`
2. `nonodb_disk_used_ratio{mount="data"} > 0.85`
3. `nonodb_snapshot_age_seconds > 3600` when snapshots are enabled
4. sustained `nonodb_queue_len` growth
5. a sustained `nonodb_errors_total` rate
6. `nonodb_auth_failures_total` and `nonodb_auth_blocks_total`

`GET /metrics` remains available for existing JSON consumers and includes the
same extra `disk`, `snapshots`, and `collection_metrics` fields.
