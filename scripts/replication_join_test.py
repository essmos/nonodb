#!/usr/bin/env python3
"""Regression test: join an already-populated primary and bootstrap safely.

The replica starts without a source, then ``replication join`` configures it.
Join schedules a controlled restart with exit status 75 so snapshot bootstrap
runs before the database is opened again. The restarted replica must receive the
documents and rebuild text/fuzzy/vector ANN search sidecars.

    cargo build --locked --bins
    python3 scripts/replication_join_test.py
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
    temp = Path(tempfile.mkdtemp(prefix="nonodb-join-", dir="/var/tmp"))
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

        def stop(name: str, expected_code: int | None = None) -> int:
            proc = procs.pop(name)
            proc.wait(timeout=30)
            clients.pop(name).close()
            if expected_code is not None and proc.returncode != expected_code:
                raise RuntimeError(
                    f"{name} exited with {proc.returncode}, expected {expected_code}"
                )
            return proc.returncode

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
        for index in range(50):
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

        # The daemon builds ANN sidecars only during startup.
        procs["primary"].terminate()
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
        pairing = primary.post(
            "/v1/replication/pairings",
            headers=headers,
            json={"name": "join-test", "databases": ["default"]},
        )
        if pairing.status_code != 200:
            raise RuntimeError(f"create pairing: {pairing.status_code} {pairing.text}")
        code = pairing.json()["result"]["code"]

        # Join an already-populated primary from a replica that started with no
        # source. Join must request a restart so bootstrap runs before runtime.
        replica = start(
            "replica",
            temp / "replica-data",
            replica_port,
            {
                "NONODB_REPLICATION_ROLE": "replica",
                "NONODB_REPLICATION_BOOTSTRAP": "always",
                "NONODB_REPLICATION_BOOTSTRAP_TMP": str(temp / "bootstrap-tmp"),
                "NONODB_REPLICATION_POLL_INTERVAL": "1",
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{replica_port}",
            },
        )
        joined = replica.post(
            "/v1/replication/join",
            headers=headers,
            json={"url": f"http://127.0.0.1:{primary_port}", "code": code},
        )
        if joined.status_code != 200:
            raise RuntimeError(f"join: {joined.status_code} {joined.text}")
        payload = joined.json()["result"]
        if payload.get("restart_requested") is not True:
            raise RuntimeError(f"join did not request restart: {payload}")
        stop("replica", expected_code=75)

        # The supervisor restarts the process; source config is persisted and
        # bootstrap_startup installs the snapshot before opening the database.
        replica = start(
            "replica",
            temp / "replica-data",
            replica_port,
            {
                "NONODB_REPLICATION_ROLE": "replica",
                "NONODB_REPLICATION_BOOTSTRAP": "always",
                "NONODB_REPLICATION_BOOTSTRAP_TMP": str(temp / "bootstrap-tmp"),
                "NONODB_REPLICATION_POLL_INTERVAL": "1",
                "NONODB_REPLICATION_ADVERTISE_URL": f"http://127.0.0.1:{replica_port}",
            },
        )
        wait_count(replica, headers, 50)
        text_hits = exec_op(
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
        fuzzy_hits = exec_op(
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
        vector_hits = exec_op(
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
        if len(text_hits) != 5 or not fuzzy_hits or len(vector_hits) != 5:
            raise RuntimeError(
                f"search after join: text={len(text_hits)} fuzzy={len(fuzzy_hits)} vector={len(vector_hits)}"
            )
        replica_log = (temp / "replica.log").read_text(
            encoding="utf-8", errors="replace"
        )
        if "rebuilt replica search sidecars" not in replica_log:
            raise RuntimeError("replica did not rebuild search sidecars after join")

        success = True
        print(
            json.dumps(
                {
                    "ok": True,
                    "restart_exit_code": 75,
                    "docs": 50,
                    "text_hits": len(text_hits),
                    "fuzzy_hits": len(fuzzy_hits),
                    "vector_hits": len(vector_hits),
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
