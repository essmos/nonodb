#!/usr/bin/env python3
"""Network partition: a live primary is fenced, a new primary wins, then rejoins.

The test uses small TCP proxies to cut the primary from voters/replicas while
clients can still reach the primary directly. This exercises lease-gated writes,
quorum election, source discovery and rejoin after the link returns.
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


class Proxy:
    def __init__(self, target_port: int):
        self.target_port = target_port
        self.listener: socket.socket | None = None
        self.port = free_port()
        self.active: list[socket.socket] = []
        self.lock = threading.Lock()
        self.thread: threading.Thread | None = None
        self.running = False

    def start(self) -> None:
        if self.running:
            return
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", self.port))
        listener.listen(64)
        listener.settimeout(0.3)
        self.listener = listener
        self.running = True
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.running = False
        if self.listener:
            try:
                self.listener.close()
            except OSError:
                pass
            self.listener = None
        with self.lock:
            active = list(self.active)
            self.active.clear()
        for sock in active:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        if self.thread:
            self.thread.join(timeout=2)
            self.thread = None

    def _accept(self) -> None:
        while self.running and self.listener:
            try:
                client, _ = self.listener.accept()
            except (socket.timeout, OSError):
                continue
            try:
                upstream = socket.create_connection(("127.0.0.1", self.target_port), timeout=2)
            except OSError:
                client.close()
                continue
            with self.lock:
                self.active.extend([client, upstream])
            threading.Thread(
                target=self._relay, args=(client, upstream), daemon=True
            ).start()
            threading.Thread(
                target=self._relay, args=(upstream, client), daemon=True
            ).start()

    def _relay(self, left: socket.socket, right: socket.socket) -> None:
        try:
            while self.running:
                data = left.recv(65536)
                if not data:
                    break
                right.sendall(data)
        except OSError:
            pass
        finally:
            for sock in (left, right):
                try:
                    sock.close()
                except OSError:
                    pass


def wait_health(client: httpx.Client, proc: subprocess.Popen[bytes], name: str) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"{name} exited with {proc.returncode}")
        try:
            if client.get("/healthz", timeout=1).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"{name} did not become healthy")


def wait_for(fn, name: str, timeout: float = 45):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
            last = value
        except Exception as exc:  # noqa: BLE001 - test diagnostics
            last = exc
        time.sleep(0.2)
    raise AssertionError(f"timeout waiting for {name}: {last}")


def main() -> int:
    temp = Path(tempfile.mkdtemp(prefix="nonodb-partition-", dir="/var/tmp"))
    auth = temp / "auth.json"
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
    ports = {name: free_port() for name in ("primary", "candidate1", "candidate2")}
    # Proxies used by the primary to reach candidates.
    primary_voter_proxies = {
        name: Proxy(ports[name]) for name in ("candidate1", "candidate2")
    }
    # Shared proxy used by candidates/replicas to reach the primary.
    primary_link = Proxy(ports["primary"])
    for proxy in [*primary_voter_proxies.values(), primary_link]:
        proxy.start()

    primary_voters = ",".join(
        [
            f"http://127.0.0.1:{ports['primary']}",
            *[
                f"http://127.0.0.1:{proxy.port}"
                for proxy in primary_voter_proxies.values()
            ],
        ]
    )
    replica_voters = ",".join(
        [
            f"http://127.0.0.1:{ports['candidate1']}",
            f"http://127.0.0.1:{ports['candidate2']}",
            f"http://127.0.0.1:{primary_link.port}",
        ]
    )
    procs: dict[str, subprocess.Popen[bytes]] = {}
    logs: list = []
    clients: dict[str, httpx.Client] = {}

    def start(name: str, role: str, voters: str, extra: dict[str, str]) -> None:
        data = temp / f"{name}-data"
        data.mkdir()
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(data),
            NONODB_SOCK=str(temp / f"{name}.sock"),
            NONODB_TCP=f"127.0.0.1:{ports[name]}",
            NONODB_AUTH_FILE=str(auth),
            NONODB_LOG="warn",
            NONODB_REPLICATION_ENABLED="1",
            NONODB_REPLICATION_ORIGIN=name,
            NONODB_REPLICATION_NODE_ID=name,
            NONODB_REPLICATION_ROLE=role,
            NONODB_REPLICATION_ADVERTISE_URL=f"http://127.0.0.1:{ports[name]}",
            NONODB_REPLICATION_VOTERS=voters,
            NONODB_REPLICATION_VOTER_TOKEN=token,
            NONODB_REPLICATION_LEASE_INTERVAL="1",
            NONODB_REPLICATION_LEASE_TTL="4",
            NONODB_REPLICATION_FAILOVER_TIMEOUT="2",
            NONODB_REPLICATION_ELECTION_INTERVAL="1",
            **extra,
        )
        log = open(temp / f"{name}.log", "wb")
        logs.append(log)
        proc = subprocess.Popen([str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT)
        procs[name] = proc
        client = httpx.Client(base_url=f"http://127.0.0.1:{ports[name]}", timeout=20)
        clients[name] = client
        wait_health(client, proc, name)

    try:
        start("primary", "primary", primary_voters, {})
        candidate_extra = {
            "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{primary_link.port}",
            "NONODB_REPLICATION_SOURCE_TOKEN": token,
            "NONODB_REPLICATION_AUTO_FAILOVER": "1",
            "NONODB_REPLICATION_CANDIDATE": "1",
        }
        start(
            "candidate1",
            "replica",
            replica_voters,
            {**candidate_extra, "NONODB_REPLICATION_CANDIDATE_PRIORITY": "0"},
        )
        start(
            "candidate2",
            "replica",
            replica_voters,
            {**candidate_extra, "NONODB_REPLICATION_CANDIDATE_PRIORITY": "5"},
        )

        primary = clients["primary"]
        wait_for(
            lambda: primary.get("/v1/replication/status", headers=headers)
            .json()["result"]
            .get("lease_valid"),
            "initial primary lease",
        )
        inserted = primary.post(
            "/v1/exec",
            headers=headers,
            json={"op": "insert", "coll": "orders", "doc": {"_id": "before", "n": 1}},
        )
        assert inserted.status_code == 200, inserted.text
        wait_for(
            lambda: all(
                clients[name]
                .post(
                    "/v1/exec",
                    headers=headers,
                    json={"op": "count", "coll": "orders", "filter": {}},
                )
                .json()
                .get("result", {})
                .get("count")
                == 1
                for name in ("candidate1", "candidate2")
            ),
            "initial replication",
        )

        # Cut the primary from voters and cut candidates from their source.
        for proxy in [*primary_voter_proxies.values(), primary_link]:
            proxy.stop()

        wait_for(
            lambda: primary.get("/v1/replication/status", headers=headers)
            .json()["result"]
            .get("role")
            == "replica",
            "isolated primary self-demotion",
            timeout=20,
        )
        blocked = primary.post(
            "/v1/exec",
            headers=headers,
            json={"op": "insert", "coll": "orders", "doc": {"_id": "stale", "n": 2}},
        )
        assert blocked.status_code == 403, blocked.text

        new_primary_name = wait_for(
            lambda: next(
                (
                    name
                    for name in ("candidate1", "candidate2")
                    if clients[name]
                    .get("/v1/replication/status", headers=headers)
                    .json()["result"]
                    .get("role")
                    == "primary"
                    and clients[name]
                    .get("/v1/replication/status", headers=headers)
                    .json()["result"]
                    .get("lease_valid")
                ),
                None,
            ),
            "new quorum primary",
            timeout=40,
        )
        new_primary = clients[new_primary_name]
        wait_for(
            lambda: new_primary.post(
                "/v1/exec",
                headers=headers,
                json={"op": "insert", "coll": "orders", "doc": {"_id": "after"}},
            ).status_code
            == 200,
            "new primary write",
        )

        # Restore the network. The old primary should discover the new primary
        # and rejoin as a replica without a manual source command.
        for proxy in [*primary_voter_proxies.values(), primary_link]:
            proxy.start()
        wait_for(
            lambda: (
                lambda status: status.get("role") == "replica"
                and status.get("replication", {}).get("source")
            )(primary.get("/v1/replication/status", headers=headers).json()["result"]),
            "old primary source discovery",
            timeout=30,
        )
        wait_for(
            lambda: primary.post(
                "/v1/exec",
                headers=headers,
                json={"op": "count", "coll": "orders", "filter": {}},
            )
            .json()
            .get("result", {})
            .get("count", 0)
            >= 2,
            "old primary catch-up",
            timeout=30,
        )
        status = primary.get("/v1/replication/status", headers=headers).json()["result"]
        assert status["read_only"] is True, status
        print(
            json.dumps(
                {
                    "ok": True,
                    "old_role": status["role"],
                    "old_source": status["replication"]["source"],
                    "new_primary": new_primary_name,
                    "converged": True,
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        for proxy in [*primary_voter_proxies.values(), primary_link]:
            proxy.stop()
        for client in clients.values():
            client.close()
        for proc in procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in procs.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        for log in logs:
            log.close()
        print(f"logs: {temp}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
