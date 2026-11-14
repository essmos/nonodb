#!/usr/bin/env python3
"""Multi-host replication test with dynamic joins and failover.

This test starts a primary, continuously writes while new replicas join via
snapshot bootstrap, verifies acks and convergence, then kills the primary and
checks automatic election, source propagation, and post-failover replication.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import threading
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


def wait_health(client: httpx.Client, name: str, proc: subprocess.Popen, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
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


def count(client: httpx.Client, headers: dict[str, str]) -> int:
    response = client.post(
        "/v1/exec",
        headers=headers,
        json={"op": "count", "coll": "events", "filter": {}},
    )
    assert response.status_code == 200, response.text
    return int(response.json()["result"]["count"])


def wait_count(
    client: httpx.Client,
    headers: dict[str, str],
    name: str,
    expected: int,
    timeout: float = 30,
) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = count(client, headers)
            if last == expected:
                return
        except (httpx.HTTPError, AssertionError):
            pass
        time.sleep(0.1)
    raise RuntimeError(f"{name} did not reach {expected} docs (last={last})")


def wait_source(
    client: httpx.Client,
    headers: dict[str, str],
    name: str,
    source: str,
    timeout: float = 30,
) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = client.get("/v1/replication/status", headers=headers).json()["result"]
            if last["replication"].get("source") == source:
                return
        except (httpx.HTTPError, KeyError):
            pass
        time.sleep(0.1)
    raise RuntimeError(f"{name} did not switch source to {source}: {last}")


def wait_role(
    client: httpx.Client,
    headers: dict[str, str],
    name: str,
    role: str,
    timeout: float = 30,
) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = client.get("/v1/replication/status", headers=headers).json()["result"]
            if last["role"] == role:
                return last
        except (httpx.HTTPError, KeyError):
            pass
        time.sleep(0.1)
    raise RuntimeError(f"{name} did not become {role}: {last}")


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing debug binaries; run cargo build --locked --bins")
    temp = Path(tempfile.mkdtemp(prefix="nonodb-cluster-", dir="/var/tmp"))
    auth = temp / "auth.json"
    procs: dict[str, subprocess.Popen] = {}
    logs: list = []
    clients: dict[str, httpx.Client] = {}
    writer_stop = threading.Event()
    writer_errors: list[str] = []
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
        headers = {"Authorization": f"Bearer {token}"}

        names = ("primary", "candidate1", "candidate2", "follower1", "follower2")
        ports = {name: free_port() for name in names}
        voters = ",".join(
            f"http://127.0.0.1:{ports[name]}"
            for name in ("primary", "candidate1", "candidate2")
        )
        followers = (
            f"http://127.0.0.1:{ports['follower1']},"
            f"http://127.0.0.1:{ports['follower2']}"
        )

        def start(name: str, role: str, extra: dict[str, str]) -> None:
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
                NONODB_REPLICATION_ROLE=role,
                NONODB_REPLICATION_NODE_ID=name,
                NONODB_REPLICATION_VOTERS=voters,
                NONODB_REPLICATION_VOTER_TOKEN=token,
                NONODB_LOG="warn",
                **extra,
            )
            log = open(temp / f"{name}.log", "w", encoding="utf-8")
            logs.append(log)
            proc = subprocess.Popen(
                [str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT
            )
            procs[name] = proc
            client = httpx.Client(base_url=f"http://127.0.0.1:{ports[name]}", timeout=30.0)
            clients[name] = client
            wait_health(client, name, proc)

        start(
            "primary",
            "primary",
            {
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_LEASE_INTERVAL": "5",
                "NONODB_REPLICATION_LEASE_TTL": "300",
            },
        )
        primary = clients["primary"]
        primary.post(
            "/v1/exec",
            headers=headers,
            json={"op": "create_collection", "coll": "events"},
        )

        def writer() -> None:
            next_id = 0
            with httpx.Client(
                base_url=f"http://127.0.0.1:{ports['primary']}", timeout=30.0
            ) as client:
                while not writer_stop.is_set():
                    response = client.post(
                        "/v1/exec",
                        headers=headers,
                        json={
                            "op": "insert",
                            "coll": "events",
                            "doc": {"_id": f"e{next_id}", "n": next_id},
                        },
                    )
                    if response.status_code != 200:
                        writer_errors.append(response.text)
                    next_id += 1
                    time.sleep(0.02)

        thread = threading.Thread(target=writer, daemon=True)
        thread.start()
        time.sleep(0.4)

        start(
            "candidate1",
            "replica",
            {
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports['candidate1']}",
                "NONODB_REPLICATION_AUTO_FAILOVER": "1",
                "NONODB_REPLICATION_CANDIDATE": "1",
                "NONODB_REPLICATION_CANDIDATE_PRIORITY": "0",
                "NONODB_REPLICATION_ELECTION_INTERVAL": "1",
                "NONODB_REPLICATION_FAILOVER_TIMEOUT": "5",
                "NONODB_REPLICATION_FOLLOWERS": followers,
            },
        )
        start(
            "follower1",
            "replica",
            {
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports['follower1']}",
            },
        )
        time.sleep(1)

        start(
            "candidate2",
            "replica",
            {
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports['candidate2']}",
                "NONODB_REPLICATION_AUTO_FAILOVER": "1",
                "NONODB_REPLICATION_CANDIDATE": "1",
                "NONODB_REPLICATION_CANDIDATE_PRIORITY": "10",
                "NONODB_REPLICATION_ELECTION_INTERVAL": "1",
                "NONODB_REPLICATION_FAILOVER_TIMEOUT": "5",
            },
        )
        start(
            "follower2",
            "replica",
            {
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{ports['follower2']}",
            },
        )
        time.sleep(2)

        writer_stop.set()
        thread.join(timeout=10)
        assert not writer_errors, writer_errors[:3]
        expected = count(primary, headers)
        assert expected > 20, expected
        for name in names[1:]:
            wait_count(clients[name], headers, name, expected)

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            acks = primary.get("/v1/replication/acks", headers=headers).json()["result"]["acks"]
            nodes = {ack["node"] for ack in acks}
            if nodes == set(names[1:]):
                break
            time.sleep(0.2)
        else:
            raise RuntimeError(f"primary did not see all replica acks: {acks}")

        procs["primary"].terminate()
        procs["primary"].wait(timeout=10)
        clients["primary"].close()

        elected = wait_role(clients["candidate1"], headers, "candidate1", "primary", 30)
        assert elected["epoch"] >= 1, elected
        expected_url = f"http://127.0.0.1:{ports['candidate1']}"
        for name in ("candidate2", "follower1", "follower2"):
            wait_source(clients[name], headers, name, expected_url)

        for index in range(20):
            response = clients["candidate1"].post(
                "/v1/exec",
                headers=headers,
                json={
                    "op": "insert",
                    "coll": "events",
                    "doc": {"_id": f"after-failover-{index}", "n": index},
                },
            )
            assert response.status_code == 200, response.text
        final_expected = expected + 20
        for name in ("candidate2", "follower1", "follower2"):
            wait_count(clients[name], headers, name, final_expected)

        print(
            json.dumps(
                {
                    "ok": True,
                    "docs_before_failover": expected,
                    "docs_after_failover": final_expected,
                    "new_primary": "candidate1",
                    "source_switched": ["candidate2", "follower1", "follower2"],
                    "replicas": list(names[1:]),
                }
            )
        )
        return 0
    finally:
        writer_stop.set()
        for proc in procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in procs.values():
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        for client in clients.values():
            client.close()
        for log in logs:
            log.close()
        import shutil

        shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
