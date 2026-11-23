#!/usr/bin/env python3
"""Graph adjacency stays consistent on a replicating follower."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))
from nonodb import NonoDB  # noqa: E402


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_health(client: httpx.Client, proc: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("daemon exited")
        try:
            if client.get("/healthz", timeout=1).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise RuntimeError("daemon did not become healthy")


def wait_for(fn, name: str, timeout: float = 30):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
            last = value
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.1)
    raise AssertionError(f"timeout waiting for {name}: {last}")


def children(node: dict) -> list[str]:
    out = []
    for child in node.get("children", []):
        out.append(child["node"]["_id"])
        out.extend(children(child))
    return out


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    cli = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))
    if not daemon.is_file() or not cli.is_file():
        raise SystemExit("missing debug binaries")
    with tempfile.TemporaryDirectory(prefix="nonodb-graph-repl-") as td:
        tmp = Path(td)
        auth = tmp / "auth.json"
        env_auth = {**os.environ, "NONODB_AUTH_FILE": str(auth)}
        subprocess.run([str(cli), "auth", "init"], env=env_auth, check=True, capture_output=True)
        token = subprocess.run(
            [str(cli), "auth", "token", "create", "admin"],
            env=env_auth, check=True, capture_output=True, text=True,
        ).stdout.strip()
        ports = {"primary": free_port(), "replica": free_port()}
        procs: list[subprocess.Popen[bytes]] = []
        clients: list[httpx.Client] = []

        def start(name: str, role: str, extra: dict[str, str]) -> None:
            data = tmp / f"{name}-data"
            env = os.environ.copy()
            env.update(
                NONODB_DATA=str(data),
                NONODB_SOCK=str(tmp / f"{name}.sock"),
                NONODB_TCP=f"127.0.0.1:{ports[name]}",
                NONODB_AUTH_FILE=str(auth),
                NONODB_LOG="warn",
                NONODB_MAINTENANCE_INTERVAL_MS="100",
                NONODB_GRAPH_REBUILD_CHUNK="1",
                NONODB_REPLICATION_ENABLED="1",
                NONODB_REPLICATION_ORIGIN=name,
                NONODB_REPLICATION_NODE_ID=name,
                NONODB_REPLICATION_ROLE=role,
                NONODB_REPLICATION_ADVERTISE_URL=f"http://127.0.0.1:{ports[name]}",
                **extra,
            )
            proc = subprocess.Popen([str(daemon)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            procs.append(proc)
            client = httpx.Client(base_url=f"http://127.0.0.1:{ports[name]}", timeout=30)
            clients.append(client)
            wait_health(client, proc)

        try:
            start("primary", "primary", {})
            start(
                "replica",
                "replica",
                {
                    "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{ports['primary']}",
                    "NONODB_REPLICATION_SOURCE_TOKEN": token,
                },
            )
            primary = NonoDB(base_url=f"http://127.0.0.1:{ports['primary']}", token=token, timeout=30)
            replica = NonoDB(base_url=f"http://127.0.0.1:{ports['replica']}", token=token, timeout=30)
            primary.create_collection("nodes")
            primary.create_collection("edges")
            pn = primary.collection("nodes")
            pe = primary.collection("edges")
            pn.insert_many([{"_id": "n1"}, {"_id": "n2"}, {"_id": "n3"}, {"_id": "n4"}])
            pn.set_graph("edges")
            pe.insert({"_id": "e1", "from": "n1", "to": "n2"})
            pe.insert({"_id": "e2", "from": "n2", "to": "n3"})

            def replica_graph_ready() -> bool:
                return replica.collection("nodes").get_graph() is not None and replica.collection("nodes").traverse(
                    "n1", max_depth=2
                )["edges"] == 2

            wait_for(replica_graph_ready, "graph replication")
            rn = replica.collection("nodes")
            re = replica.collection("edges")
            assert children(rn.traverse("n1", max_depth=2)["roots"][0]) == ["n2", "n3"]

            pe.update({"_id": "e2"}, {"$set": {"to": "n4"}})
            wait_for(
                lambda: set(children(rn.traverse("n1", max_depth=2)["roots"][0])) == {"n2", "n4"},
                "edge update replication",
            )
            pe.delete({"_id": "e1"}, multi=False)
            wait_for(
                lambda: children(rn.traverse("n1", max_depth=2)["roots"][0]) == [],
                "edge delete replication",
            )

            pe.insert({"_id": "e3", "from": "n1", "to": "n3"})
            wait_for(
                lambda: children(rn.traverse("n1", max_depth=1)["roots"][0]) == ["n3"],
                "edge insert replication",
            )
            pe.insert({"_id": "e4", "from": "n1", "to": "n4"})
            wait_for(
                lambda: len(children(rn.traverse("n1", max_depth=1)["roots"][0])) == 2,
                "second edge insert replication",
            )
            status = pn.reindex_graph(online=True)
            assert status["done"] is False
            wait_for(lambda: pn.graph_rebuild_status()["done"], "primary online rebuild")
            wait_for(
                lambda: replica.collection("nodes").get_graph()["generation"]
                == pn.get_graph()["generation"],
                "generation replication",
            )
            assert set(children(rn.traverse("n1", max_depth=1)["roots"][0])) == {"n3", "n4"}
            print("graph replication tests passed")
            return 0
        finally:
            for proc in procs:
                proc.terminate()
            for proc in procs:
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
            for client in clients:
                client.close()


if __name__ == "__main__":
    raise SystemExit(main())
