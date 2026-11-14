#!/usr/bin/env python3
"""Verify that a primary self-demotes after losing its quorum lease."""

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
    temp = Path(tempfile.mkdtemp(prefix="nonodb-lease-", dir="/var/tmp"))
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
        voter_port = free_port()
        voters = f"http://127.0.0.1:{primary_port},http://127.0.0.1:{voter_port}"
        for name, port, role, extra in [
            (
                "primary",
                primary_port,
                "primary",
                {"NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{primary_port}"},
            ),
            (
                "voter",
                voter_port,
                "replica",
                {
                    "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{primary_port}",
                    "NONODB_REPLICATION_SOURCE_TOKEN": token,
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
                NONODB_REPLICATION_VOTERS=voters,
                NONODB_REPLICATION_VOTER_TOKEN=token,
                NONODB_REPLICATION_LEASE_INTERVAL="1",
                NONODB_REPLICATION_LEASE_TTL="3",
                NONODB_LOG="warn",
                **extra,
            )
            log = open(temp / f"{name}.log", "w", encoding="utf-8")
            logs.append(log)
            procs.append(
                subprocess.Popen([str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT)
            )

        headers = {"Authorization": f"Bearer {token}"}
        client = httpx.Client(base_url=f"http://127.0.0.1:{primary_port}", timeout=30.0)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                if client.get("/healthz").status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        else:
            raise RuntimeError("primary did not become healthy")

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            status = client.get("/v1/replication/status", headers=headers).json()["result"]
            if status["role"] == "primary":
                break
            time.sleep(0.1)
        else:
            raise RuntimeError(f"primary did not start as primary: {status}")

        procs[1].terminate()
        procs[1].wait(timeout=10)

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            status = client.get("/v1/replication/status", headers=headers).json()["result"]
            if status["role"] == "replica":
                break
            time.sleep(0.2)
        else:
            raise RuntimeError(f"primary did not self-demote after losing quorum: {status}")
        assert status["read_only"] is True, status
        assert (temp / "primary-data" / "replication-lease.json").is_file()
        print(json.dumps({"ok": True, "role": status["role"], "epoch": status["epoch"]}))
        return 0
    finally:
        for proc in procs:
            if proc.poll() is None:
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
