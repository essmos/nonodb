#!/usr/bin/env python3
"""Replica rediscovers the primary through stable seeds after source loss."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
CLI = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def run_cli(auth: Path, *args: str) -> str:
    env = os.environ.copy()
    env["NONODB_AUTH_FILE"] = str(auth)
    proc = subprocess.run(
        [str(CLI), *args], env=env, capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        raise RuntimeError(f"nonodb-cli {' '.join(args)}: {proc.stderr.strip()}")
    return proc.stdout.strip()


def wait_healthy(client: httpx.Client, proc: subprocess.Popen, name: str) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"{name} exited with {proc.returncode}")
        try:
            if client.get("/healthz").status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"{name} did not become healthy")


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing debug binaries; run cargo build --locked --bins")
    temp = Path(tempfile.mkdtemp(prefix="nonodb-discovery-", dir="/var/tmp"))
    procs: list[subprocess.Popen] = []
    logs = []
    try:
        auth = temp / "auth.json"
        run_cli(auth, "auth", "init")
        admin = run_cli(auth, "auth", "token", "create", "admin")
        headers = {"Authorization": f"Bearer {admin}"}
        primary_port, replica_port, dead_port = free_port(), free_port(), free_port()

        primary_data = temp / "primary-data"
        replica_data = temp / "replica-data"
        primary_env = os.environ.copy()
        primary_env.update(
            NONODB_DATA=str(primary_data),
            NONODB_SOCK=str(temp / "primary.sock"),
            NONODB_TCP=f"127.0.0.1:{primary_port}",
            NONODB_AUTH_FILE=str(auth),
            NONODB_REPLICATION_ENABLED="1",
            NONODB_REPLICATION_ORIGIN="discovery-primary",
            NONODB_REPLICATION_ROLE="primary",
            NONODB_REPLICATION_ADVERTISE_URL=f"http://127.0.0.1:{primary_port}",
            NONODB_LOG="warn",
        )
        log = open(temp / "primary.log", "w", encoding="utf-8")
        logs.append(log)
        primary = subprocess.Popen(
            [str(DAEMON)], env=primary_env, stdout=log, stderr=subprocess.STDOUT
        )
        procs.append(primary)

        replica_env = os.environ.copy()
        replica_env.update(
            NONODB_DATA=str(replica_data),
            NONODB_SOCK=str(temp / "replica.sock"),
            NONODB_TCP=f"127.0.0.1:{replica_port}",
            NONODB_AUTH_FILE=str(auth),
            NONODB_REPLICATION_ENABLED="1",
            NONODB_REPLICATION_ORIGIN="discovery-replica",
            NONODB_REPLICATION_ROLE="replica",
            NONODB_REPLICATION_SOURCE=f"http://127.0.0.1:{dead_port}",
            NONODB_REPLICATION_SOURCE_TOKEN=admin,
            NONODB_REPLICATION_VOTER_TOKEN=admin,
            NONODB_REPLICATION_SEEDS=f"http://127.0.0.1:{primary_port}",
            NONODB_REPLICATION_POLL_INTERVAL="1",
            NONODB_LOG="warn",
        )
        log = open(temp / "replica.log", "w", encoding="utf-8")
        logs.append(log)
        replica = subprocess.Popen(
            [str(DAEMON)], env=replica_env, stdout=log, stderr=subprocess.STDOUT
        )
        procs.append(replica)

        with httpx.Client(
            base_url=f"http://127.0.0.1:{primary_port}", timeout=30.0
        ) as client:
            wait_healthy(client, primary, "primary")
            response = client.post(
                "/v1/exec",
                headers=headers,
                json={
                    "op": "insert",
                    "coll": "orders",
                    "doc": {"_id": "seed-1", "value": 42},
                },
            )
            assert response.status_code == 200, response.text

        with httpx.Client(
            base_url=f"http://127.0.0.1:{replica_port}", timeout=30.0
        ) as replica_client:
            wait_healthy(replica_client, replica, "replica")
            deadline = time.monotonic() + 20
            status = {}
            while time.monotonic() < deadline:
                status = replica_client.get(
                    "/v1/replication/status", headers=headers
                ).json()["result"]
                source = status.get("replication", {}).get("source")
                if source == f"http://127.0.0.1:{primary_port}":
                    found = replica_client.post(
                        "/v1/exec",
                        headers=headers,
                        json={"op": "find", "coll": "orders", "filter": {"_id": "seed-1"}},
                    )
                    if found.status_code == 200 and found.json()["result"]:
                        print(
                            json.dumps(
                                {
                                    "ok": True,
                                    "source": source,
                                    "rediscovered": True,
                                    "role": status.get("role"),
                                }
                            )
                        )
                        return 0
                time.sleep(0.25)
            raise RuntimeError(f"replica did not rediscover primary: {status}")
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        for log in logs:
            log.close()
        print(f"logs: {temp}")


if __name__ == "__main__":
    raise SystemExit(main())
