#!/usr/bin/env python3
"""Replica snapshot bootstrap with segment-backed search sidecars.

Starts a primary with text/fuzzy segment indexes, creates a replica, forces a
snapshot bootstrap, then verifies the replica rebuilt the segment files from the
snapshot LMDB data and serves text and fuzzy queries.

    cargo build --locked --bins
    python3 scripts/replication_sidecar_test.py
"""

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


def wait_health(
    client: httpx.Client, name: str, proc: subprocess.Popen, timeout: float = 30
) -> None:
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


def exec_op(client: httpx.Client, headers: dict[str, str], body: dict) -> dict:
    response = client.post("/v1/exec", headers=headers, json=body)
    if response.status_code != 200:
        raise RuntimeError(
            f"{body['op']} failed: {response.status_code} {response.text}"
        )
    return response.json()["result"]


def wait_count(
    client: httpx.Client, headers: dict[str, str], expected: int, timeout: float = 60
) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = exec_op(
                client, headers, {"op": "count", "coll": "events", "filter": {}}
            )["count"]
            if last == expected:
                return
        except (httpx.HTTPError, RuntimeError):
            pass
        time.sleep(0.1)
    raise RuntimeError(f"replica count did not reach {expected}: {last}")


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing debug binaries; run cargo build --locked --bins")
    temp = Path(tempfile.mkdtemp(prefix="nonodb-repl-sidecar-", dir="/var/tmp"))
    auth = temp / "auth.json"
    procs: dict[str, subprocess.Popen] = {}
    clients: dict[str, httpx.Client] = {}
    logs: list = []
    success = False
    try:
        env_auth = {**os.environ, "NONODB_AUTH_FILE": str(auth)}
        subprocess.run(
            [str(CLI), "auth", "init"], env=env_auth, check=True, capture_output=True
        )
        token = subprocess.run(
            [str(CLI), "auth", "token", "create", "admin"],
            env=env_auth,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        headers = {"Authorization": f"Bearer {token}"}

        def start(name: str, data: Path, port: int, extra: dict[str, str]) -> httpx.Client:
            env = os.environ.copy()
            env.update(
                {
                    "NONODB_DATA": str(data),
                    "NONODB_SOCK": str(temp / f"{name}.sock"),
                    "NONODB_TCP": f"127.0.0.1:{port}",
                    "NONODB_AUTH_FILE": str(auth),
                    "NONODB_REPLICATION_ENABLED": "1",
                    "NONODB_REPLICATION_ORIGIN": f"{name}-origin",
                    "NONODB_REPLICATION_NODE_ID": name,
                    "NONODB_LOG": "info",
                    **extra,
                }
            )
            log = open(temp / f"{name}.log", "a", encoding="utf-8")
            logs.append(log)
            proc = subprocess.Popen(
                [str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT
            )
            procs[name] = proc
            client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=60.0)
            clients[name] = client
            wait_health(client, name, proc)
            return client

        def stop(name: str) -> None:
            proc = procs.pop(name)
            proc.terminate()
            proc.wait(timeout=10)
            clients.pop(name).close()

        primary_port = free_port()
        replica_port = free_port()
        primary = start(
            "primary",
            temp / "primary-data",
            primary_port,
            {
                "NONODB_REPLICATION_ROLE": "primary",
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{primary_port}",
            },
        )
        exec_op(primary, headers, {"op": "create_collection", "coll": "events"})
        for index in range(100):
            exec_op(
                primary,
                headers,
                {
                    "op": "insert",
                    "coll": "events",
                    "doc": {
                        "_id": f"e{index}",
                        "title": f"Document {index}",
                        "body": "rust async search replication document",
                        "embedding": [index, index + 1, index + 2, index + 3],
                    },
                },
            )
        exec_op(
            primary,
            headers,
            {"op": "create_text_index", "coll": "events", "field": "body"},
        )
        exec_op(
            primary,
            headers,
            {"op": "create_fuzzy_index", "coll": "events", "field": "title"},
        )
        exec_op(
            primary,
            headers,
            {
                "op": "create_vector_index",
                "coll": "events",
                "field": "embedding",
                "dims": 4,
                "metric": "l2",
            },
        )
        segment_files = list(
            (temp / "primary-data" / "default" / "search").rglob("*.seg")
        )
        if not segment_files:
            raise RuntimeError("primary did not create segment sidecars")

        # The daemon builds ANN sidecars only during startup.
        stop("primary")
        primary = start(
            "primary",
            temp / "primary-data",
            primary_port,
            {
                "NONODB_REPLICATION_ROLE": "primary",
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{primary_port}",
                "NONODB_BUILD_VECTOR_ANN": "1",
                "NONODB_VECTOR_ANN_PROBE": "4",
                "NONODB_VECTOR_ANN_COLLECTIONS": "events",
            },
        )
        ann_files = list(
            (temp / "primary-data" / "default" / "vector-ann").rglob("*.ivf")
        )
        if not ann_files:
            raise RuntimeError("primary did not create a vector ANN sidecar")

        replica = start(
            "replica",
            temp / "replica-data",
            replica_port,
            {
                "NONODB_REPLICATION_ROLE": "replica",
                "NONODB_REPLICATION_SOURCE": f"http://127.0.0.1:{primary_port}",
                "NONODB_REPLICATION_SOURCE_TOKEN": token,
                "NONODB_REPLICATION_BOOTSTRAP": "always",
                "NONODB_REPLICATION_POLL_INTERVAL": "1",
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{replica_port}",
            },
        )

        wait_count(replica, headers, 100)
        hits = exec_op(
            replica,
            headers,
            {
                "op": "search",
                "coll": "events",
                "field": "body",
                "query": "rust async",
                "limit": 5,
            },
        )["results"]
        if len(hits) != 5:
            raise RuntimeError(f"replica text search returned {len(hits)} hits")
        fuzzy = exec_op(
            replica,
            headers,
            {
                "op": "fuzzy",
                "coll": "events",
                "field": "title",
                "query": "documnt",
                "max_distance": 2,
                "limit": 5,
            },
        )["results"]
        if not fuzzy:
            raise RuntimeError("replica fuzzy search returned no hits")
        vectors = exec_op(
            replica,
            headers,
            {
                "op": "vector_search",
                "coll": "events",
                "field": "embedding",
                "vector": [0.0, 0.0, 0.0, 0.0],
                "limit": 5,
            },
        )["results"]
        if len(vectors) != 5:
            raise RuntimeError(f"replica vector search returned {len(vectors)} hits")
        replica_ann = list(
            (temp / "replica-data" / "default" / "vector-ann").rglob("*.ivf")
        )
        if not replica_ann:
            raise RuntimeError("replica did not rebuild the vector ANN sidecar")

        replica_log = (temp / "replica.log").read_text(
            encoding="utf-8", errors="replace"
        )
        if "rebuilt replica search sidecars" not in replica_log:
            raise RuntimeError("replica did not log sidecar rebuild")

        success = True
        print(
            json.dumps(
                {
                    "ok": True,
                    "docs": 100,
                    "primary_segment_files": len(segment_files),
                    "primary_ann_files": len(ann_files),
                    "replica_text_hits": len(hits),
                    "replica_fuzzy_hits": len(fuzzy),
                    "replica_vector_hits": len(vectors),
                    "replica_ann_files": len(replica_ann),
                    "replica_log_rebuild": True,
                }
            )
        )
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
        for client in clients.values():
            client.close()
        for log in logs:
            log.close()
        if success:
            import shutil

            shutil.rmtree(temp, ignore_errors=True)
        else:
            print(f"logs retained in {temp}")


if __name__ == "__main__":
    raise SystemExit(main())
