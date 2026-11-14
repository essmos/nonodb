#!/usr/bin/env python3
"""Verify automatic quorum election after the primary disappears."""

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
    temp = Path(tempfile.mkdtemp(prefix="nonodb-election-", dir="/var/tmp"))
    auth = temp / "auth.json"
    procs: dict[str, subprocess.Popen] = {}
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

        ports = {
            name: free_port()
            for name in ("primary", "candidate", "candidate2", "voter", "follower")
        }
        voters = ",".join(
            f"http://127.0.0.1:{ports[name]}"
            for name in ("primary", "candidate", "voter")
        )
        definitions = {
            "primary": {
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_LEASE_INTERVAL": "1",
                "NONODB_REPLICATION_LEASE_TTL": "3",
            },
            "candidate": {
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports['candidate']}",
                "NONODB_REPLICATION_AUTO_FAILOVER": "1",
                "NONODB_REPLICATION_CANDIDATE": "1",
                "NONODB_REPLICATION_ELECTION_INTERVAL": "1",
                "NONODB_REPLICATION_FAILOVER_TIMEOUT": "3",
                "NONODB_REPLICATION_FOLLOWERS": (
                    f"http://127.0.0.1:{ports['follower']},"
                    f"http://127.0.0.1:{ports['candidate2']}"
                ),
            },
            "candidate2": {
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports['candidate2']}",
                "NONODB_REPLICATION_AUTO_FAILOVER": "1",
                "NONODB_REPLICATION_CANDIDATE": "1",
                "NONODB_REPLICATION_CANDIDATE_PRIORITY": "5",
                "NONODB_REPLICATION_ELECTION_INTERVAL": "1",
                "NONODB_REPLICATION_FAILOVER_TIMEOUT": "3",
            },
            "voter": {
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports['voter']}",
            },
            "follower": {
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports['follower']}",
            },
        }
        for name, extra in definitions.items():
            data = temp / f"{name}-data"
            data.mkdir()
            env = os.environ.copy()
            env.update(
                NONODB_DATA=str(data),
                NONODB_SOCK=str(temp / f"{name}.sock"),
                NONODB_TCP=f"127.0.0.1:{ports[name]}",
                NONODB_AUTH_FILE=str(auth),
                NONODB_REPLICATION_ENABLED="1",
                NONODB_REPLICATION_ORIGIN=f"{name}-origin",
                NONODB_REPLICATION_ROLE=("primary" if name == "primary" else "replica"),
                NONODB_REPLICATION_VOTERS=voters,
                NONODB_REPLICATION_VOTER_TOKEN=token,
                NONODB_REPLICATION_NODE_ID=name,
                NONODB_LOG="warn",
                **extra,
            )
            log = open(temp / f"{name}.log", "w", encoding="utf-8")
            logs.append(log)
            procs[name] = subprocess.Popen(
                [str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT
            )

        headers = {"Authorization": f"Bearer {token}"}
        clients = {}
        for name, port in ports.items():
            client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30.0)
            clients[name] = client
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

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            candidate_status = clients["candidate"].get(
                "/v1/replication/status", headers=headers
            ).json()["result"]
            voter_status = clients["voter"].get(
                "/v1/replication/status", headers=headers
            ).json()["result"]
            if (
                candidate_status["replication"].get("source")
                == f"http://127.0.0.1:{ports['primary']}"
                and voter_status["replication"].get("source")
                == f"http://127.0.0.1:{ports['primary']}"
            ):
                break
            time.sleep(0.1)
        else:
            raise RuntimeError(
                f"replicas did not attach to the primary: {candidate_status} {voter_status}"
            )
        time.sleep(1)

        procs["primary"].terminate()
        procs["primary"].wait(timeout=10)
        clients["primary"].close()

        candidate = clients["candidate"]
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            status = candidate.get("/v1/replication/status", headers=headers).json()["result"]
            if status["role"] == "primary":
                break
            time.sleep(0.2)
        else:
            raise RuntimeError(f"candidate did not win the election: {status}")
        assert status["read_only"] is False, status
        assert status["epoch"] == 1, status
        assert status["write_url"] == f"http://127.0.0.1:{ports['candidate']}", status
        primary_info = candidate.get(
            "/v1/replication/primary", headers=headers
        ).json()["result"]
        assert primary_info["write_url"] == f"http://127.0.0.1:{ports['candidate']}", primary_info
        voter_client = clients["voter"]
        deadline = time.monotonic() + 10
        voter_status = None
        while time.monotonic() < deadline:
            voter_status = voter_client.get(
                "/v1/replication/status", headers=headers
            ).json()["result"]
            if voter_status["replication"].get("source") == (
                f"http://127.0.0.1:{ports['candidate']}"
            ):
                break
            time.sleep(0.2)
        else:
            raise RuntimeError(
                f"voter did not switch to the elected primary: {voter_status}"
            )
        follower_client = clients["follower"]
        deadline = time.monotonic() + 10
        follower_status = None
        while time.monotonic() < deadline:
            follower_status = follower_client.get(
                "/v1/replication/status", headers=headers
            ).json()["result"]
            if follower_status["replication"].get("source") == (
                f"http://127.0.0.1:{ports['candidate']}"
            ):
                break
            time.sleep(0.2)
        else:
            raise RuntimeError(
                f"follower did not switch to the elected primary: {follower_status}"
            )
        second = clients["candidate2"].get(
            "/v1/replication/status", headers=headers
        ).json()["result"]
        assert second["role"] == "replica", second
        assert second["replication"].get("source") == (
            f"http://127.0.0.1:{ports['candidate']}"
        ), second
        elected_write = candidate.get("/readyz/write")
        assert elected_write.status_code == 200, elected_write.text
        follower_write = follower_client.get("/readyz/write")
        assert follower_write.status_code == 503, follower_write.text
        assert follower_write.headers.get("location") == (
            f"http://127.0.0.1:{ports['candidate']}"
        )
        insert = candidate.post(
            "/v1/exec",
            headers=headers,
            json={"op": "insert", "coll": "orders", "doc": {"_id": "elected"}},
        )
        assert insert.status_code == 200, insert.text
        print(json.dumps({"ok": True, "role": status["role"], "epoch": status["epoch"]}))
        return 0
    finally:
        for proc in procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in procs.values():
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        for log in logs:
            log.close()
        import shutil

        if os.environ.get("NONODB_KEEP_ELECTION_TEMP"):
            print(f"kept election test directory: {temp}")
        else:
            shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
