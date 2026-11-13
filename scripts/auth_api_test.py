#!/usr/bin/env python3
"""Authorization API and per-collection metrics integration checks.

Starts an isolated daemon on loopback TCP with a temporary auth registry. The
test never prints plaintext tokens, never touches production data, and verifies:

- admin-only auth API access
- principal creation/update and scope validation
- one-time token creation and live disable
- protection against disabling the last enabled administrator
- mTLS certificate registration and disable
- per-collection request/read/write/cache/error metrics via /metrics and exec

Usage:
    cargo build --locked --bins
    python3 scripts/auth_api_test.py
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
CLI = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def run_cli(auth_file: Path, *args: str) -> str:
    env = os.environ.copy()
    env["NONODB_AUTH_FILE"] = str(auth_file)
    proc = subprocess.run(
        [str(CLI), "auth", *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"nonodb-cli auth {' '.join(args)}: {proc.stderr.strip()}")
    return proc.stdout.strip()


def wait_for_health(client: httpx.Client, proc: subprocess.Popen, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited with code {proc.returncode}")
        try:
            if client.get("/healthz").status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise RuntimeError("daemon did not become healthy")


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing debug binaries; run cargo build --locked --bins")

    temp = Path(tempfile.mkdtemp(prefix="nonodb-auth-api-", dir="/var/tmp"))
    data = temp / "data"
    data.mkdir()
    auth_file = temp / "auth.json"
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    log_file = None
    proc: subprocess.Popen | None = None

    try:
        run_cli(auth_file, "init")
        admin_token = run_cli(auth_file, "token", "create", "admin")

        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(data),
            NONODB_SOCK=str(temp / "nonodb.sock"),
            NONODB_TCP=f"127.0.0.1:{port}",
            NONODB_AUTH_FILE=str(auth_file),
            NONODB_LOG="warn",
            NONODB_CACHE_TTL="0",
            NONODB_MAINTENANCE_INTERVAL_MS="100",
            NONODB_SNAPSHOT_DIR=str(temp / "snapshots"),
            NONODB_SNAPSHOT_INTERVAL="1",
            NONODB_SNAPSHOT_KEEP="2",
        )
        log_file = open(temp / "daemon.log", "w", encoding="utf-8")
        proc = subprocess.Popen(
            [str(DAEMON)],
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

        def headers(token: str) -> dict[str, str]:
            return {"Authorization": f"Bearer {token}"}

        with httpx.Client(base_url=base_url, timeout=5.0, trust_env=False) as client:
            wait_for_health(client, proc, timeout=15.0)

            assert client.get("/v1/auth").status_code == 401
            assert client.get("/healthz").status_code == 200
            ready = client.get("/readyz")
            assert ready.status_code == 200, ready.text
            assert ready.json()["checks"]["writer"] is True
            assert ready.json()["checks"]["storage"] is True
            cli_env = os.environ.copy()
            cli_env["NONODB_SOCK"] = str(temp / "nonodb.sock")
            assert (
                subprocess.run(
                    [str(CLI), "ready"],
                    env=cli_env,
                    capture_output=True,
                    check=False,
                ).returncode
                == 0
            )

            summary = client.get("/v1/auth", headers=headers(admin_token))
            assert summary.status_code == 200, summary.text
            assert summary.headers.get("cache-control") == "no-store"
            summary_result = summary.json()["result"]
            assert summary_result["principals"][0]["name"] == "admin"
            assert all("sha256" not in token for token in summary_result["tokens"])

            created = client.post(
                "/v1/auth/principals",
                headers=headers(admin_token),
                json={"name": "reader", "scopes": ["read:orders"]},
            )
            assert created.status_code == 200, created.text

            bad_scope = client.post(
                "/v1/auth/principals",
                headers=headers(admin_token),
                json={"name": "bad", "scopes": ["not-a-scope"]},
            )
            assert bad_scope.status_code == 400, bad_scope.text

            token_response = client.post(
                "/v1/auth/tokens",
                headers=headers(admin_token),
                json={"principal": "reader", "kid": "reader1"},
            )
            assert token_response.status_code == 200, token_response.text
            reader_token = token_response.json()["result"]["token"]
            assert reader_token.startswith("nodb_reader1_")

            assert (
                client.post(
                    "/v1/auth/principals",
                    headers=headers(admin_token),
                    json={"name": "prometheus", "scopes": ["metrics:read"]},
                ).status_code
                == 200
            )
            prometheus_token_response = client.post(
                "/v1/auth/tokens",
                headers=headers(admin_token),
                json={"principal": "prometheus", "kid": "metrics1"},
            )
            assert prometheus_token_response.status_code == 200
            prometheus_token = prometheus_token_response.json()["result"]["token"]

            assert (
                client.post(
                    "/v1/exec",
                    headers=headers(admin_token),
                    json={"op": "create_collection", "coll": "orders"},
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/v1/exec",
                    headers=headers(admin_token),
                    json={"op": "insert", "coll": "orders", "doc": {"_id": "o1", "x": 1}},
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/v1/exec",
                    headers=headers(reader_token),
                    json={"op": "find", "coll": "orders", "filter": {}},
                ).status_code
                == 200
            )

            # A second collection must not leak into a scoped principal's stats.
            assert (
                client.post(
                    "/v1/exec",
                    headers=headers(admin_token),
                    json={"op": "create_collection", "coll": "secret"},
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/v1/exec",
                    headers=headers(admin_token),
                    json={"op": "insert", "coll": "secret", "doc": {"_id": "s1"}},
                ).status_code
                == 200
            )
            stats_response = client.post(
                "/v1/exec",
                headers=headers(reader_token),
                json={"op": "database_stats"},
            )
            assert stats_response.status_code == 200, stats_response.text
            stats = stats_response.json()["result"]
            assert stats["database"] == "default"
            assert stats["total_records"] >= 1
            assert [item["collection"] for item in stats["collections"]] == ["orders"]
            assert stats["collection_bytes"] > 0
            orders = stats["collections"][0]
            assert orders["requests_last_minute"] >= 1
            assert orders["avg_requests_per_second"] > 0

            admin_stats = client.post(
                "/v1/exec",
                headers=headers(admin_token),
                json={"op": "database_stats"},
            )
            assert admin_stats.status_code == 200
            assert {
                item["collection"] for item in admin_stats.json()["result"]["collections"]
            } == {"orders", "secret"}

            collection_metrics = client.post(
                "/v1/exec",
                headers=headers(reader_token),
                json={"op": "get_collection_metrics", "coll": "orders"},
            )
            assert collection_metrics.status_code == 200, collection_metrics.text
            metrics = collection_metrics.json()["result"]
            assert metrics["requests_total"] >= 1
            assert metrics["read_ops_total"] >= 1
            assert metrics["write_requests_total"] >= 1

            all_metrics = client.get("/metrics", headers=headers(admin_token))
            assert all_metrics.status_code == 200
            rows = all_metrics.json()["collection_metrics"]
            assert any(
                row["database"] == "default" and row["collection"] == "orders"
                for row in rows
            )
            assert all_metrics.json()["disk"]["data"]["total_bytes"] > 0

            # A metrics:read principal can scrape metrics but cannot manage auth.
            assert client.get("/metrics", headers=headers(prometheus_token)).status_code == 200
            prometheus = client.get(
                "/metrics/prometheus", headers=headers(prometheus_token)
            )
            assert prometheus.status_code == 200, prometheus.text
            assert prometheus.headers["content-type"].startswith("text/plain")
            body = prometheus.text
            assert "nonodb_requests_total" in body
            assert 'nonodb_collection_requests_total{database="default",collection="orders"}' in body
            assert 'nonodb_disk_used_ratio{mount="data"}' in body
            assert client.get("/v1/auth", headers=headers(prometheus_token)).status_code == 403

            # Snapshots run in the background; wait for the first success metric.
            for _ in range(50):
                body = client.get(
                    "/metrics/prometheus", headers=headers(admin_token)
                ).text
                if 'nonodb_snapshot_success_total{database="default"}' in body:
                    break
                time.sleep(0.1)
            else:
                raise AssertionError("snapshot metrics did not appear")
            assert 'nonodb_snapshot_age_seconds{database="default"}' in body

            assert (
                client.patch(
                    "/v1/auth/tokens/reader1",
                    headers=headers(admin_token),
                    json={"disabled": True},
                ).status_code
                == 200
            )
            time.sleep(1.2)
            assert client.get("/metrics", headers=headers(reader_token)).status_code == 401

            summary = client.get("/v1/auth", headers=headers(admin_token)).json()["result"]
            admin_kid = next(
                token["kid"] for token in summary["tokens"] if token["principal"] == "admin"
            )
            last_admin = client.patch(
                f"/v1/auth/tokens/{admin_kid}",
                headers=headers(admin_token),
                json={"disabled": True},
            )
            assert last_admin.status_code == 409, last_admin.text

            assert (
                client.post(
                    "/v1/auth/principals",
                    headers=headers(admin_token),
                    json={"name": "admin2", "scopes": ["admin:db"]},
                ).status_code
                == 200
            )
            second = client.post(
                "/v1/auth/tokens",
                headers=headers(admin_token),
                json={"principal": "admin2", "kid": "admin2a"},
            )
            assert second.status_code == 200, second.text
            admin2_token = second.json()["result"]["token"]
            assert (
                client.patch(
                    f"/v1/auth/tokens/{admin_kid}",
                    headers=headers(admin_token),
                    json={"disabled": True},
                ).status_code
                == 200
            )

            assert (
                client.put(
                    "/v1/auth/principals/reader",
                    headers=headers(admin2_token),
                    json={"scopes": ["read:orders", "write:orders"]},
                ).status_code
                == 200
            )
            fingerprint = "aa" * 32
            assert (
                client.post(
                    "/v1/auth/certificates",
                    headers=headers(admin2_token),
                    json={"sha256": fingerprint, "principal": "reader"},
                ).status_code
                == 200
            )
            assert (
                client.patch(
                    f"/v1/auth/certificates/{fingerprint}",
                    headers=headers(admin2_token),
                    json={"disabled": True},
                ).status_code
                == 200
            )
            referenced = client.delete(
                "/v1/auth/principals/reader", headers=headers(admin2_token)
            )
            assert referenced.status_code == 400, referenced.text

            print("PASS  auth API, database storage stats, Prometheus, disk and snapshot metrics")
            return 0
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        if log_file is not None:
            log_file.close()
        # Temporary data is tiny; keep it for inspection only on failure.
        if sys.exc_info()[0] is None:
            __import__("shutil").rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
