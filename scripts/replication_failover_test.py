#!/usr/bin/env python3
"""Verify promotion fences and demotes the previous replication source."""

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


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing debug binaries; run cargo build --locked --bins")
    temp = Path(tempfile.mkdtemp(prefix="nonodb-failover-", dir="/var/tmp"))
    auth = temp / "auth.json"
    procs: list[subprocess.Popen] = []
    logs: list = []
    try:
        env_auth = {**os.environ, "NONODB_AUTH_FILE": str(auth)}
        subprocess.run([str(CLI), "auth", "init"], env=env_auth, check=True, capture_output=True)
        token = subprocess.run(
            [str(CLI), "auth", "token", "create", "admin"],
            env=env_auth,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

        primary_port = free_port()
        replica_port = free_port()
        for name, port, role, extra in [
            ("primary", primary_port, "primary", {}),
            (
                "replica",
                replica_port,
                "replica",
                {
                    "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{primary_port}",
                    "NONODB_REPLICATION_SOURCE_TOKEN": token,
                    "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{replica_port}",
                    "NONODB_REPLICATION_NODE_ID": "replica-1",
                },
            ),
        ]:
            data = temp / f"{name}-data"
            data.mkdir()
            env = os.environ.copy()
            env.update(
                NONODB_DATA=str(data),
                NONODB_SOCK=str(temp / f"{name}.sock"),
                NONODB_TCP=f"127.0.0.1:{port}",
                NONODB_AUTH_FILE=str(auth),
                NONODB_REPLICATION_ENABLED="1",
                NONODB_REPLICATION_ORIGIN=f"{name}-origin",
                NONODB_REPLICATION_ROLE=role,
                NONODB_REPLICATION_VOTERS=(
                    f"http://127.0.0.1:{primary_port},http://127.0.0.1:{replica_port}"
                ),
                NONODB_REPLICATION_VOTER_TOKEN=token,
                NONODB_REPLICATION_POLL_INTERVAL="1",
                NONODB_LOG="warn",
                **extra,
            )
            log = open(temp / f"{name}.log", "w", encoding="utf-8")
            logs.append(log)
            procs.append(
                subprocess.Popen([str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT)
            )

        headers = {"Authorization": f"Bearer {token}"}
        clients = []
        for name, port in [("primary", primary_port), ("replica", replica_port)]:
            client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30.0)
            clients.append(client)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    if client.get("/healthz").status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError(f"{name} did not become healthy")

        primary, replica = clients
        promoted = replica.post(
            "/v1/replication/promote",
            headers=headers,
            json={"expected_epoch": 0},
        )
        assert promoted.status_code == 200, promoted.text
        role = promoted.json()["result"]["role"]
        assert role["role"] == "primary", role
        assert role["epoch"] == 1, role
        assert (temp / "primary-data" / "replication-vote.json").is_file()
        assert (temp / "replica-data" / "replication-vote.json").is_file()

        old = primary.get("/v1/replication/status", headers=headers).json()["result"]
        assert old["role"] == "replica", old
        assert old["epoch"] == 1, old
        assert old["successor"] == f"http://127.0.0.1:{replica_port}", old

        insert = replica.post(
            "/v1/exec",
            headers=headers,
            json={"op": "insert", "coll": "orders", "doc": {"_id": "after-failover"}},
        )
        assert insert.status_code == 200, insert.text
        blocked = primary.post(
            "/v1/exec",
            headers=headers,
            json={"op": "insert", "coll": "orders", "doc": {"_id": "old-primary"}},
        )
        assert blocked.status_code == 403, blocked.text
        assert blocked.json()["error"]["code"] == "read_only", blocked.text

        print(json.dumps({"ok": True, "new_epoch": role["epoch"], "old_role": old["role"]}))
        return 0
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        for log in logs:
            log.close()
        import shutil

        shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
