"""Graph authorization: node-only readers must not traverse edges."""

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


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def run_cli(auth_file: Path, *args: str) -> str:
    env = os.environ.copy()
    env["NONODB_AUTH_FILE"] = str(auth_file)
    proc = subprocess.run(
        [str(Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))), "auth", *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip())
    return proc.stdout.strip()


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    cli = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))
    if not daemon.is_file() or not cli.is_file():
        print("missing debug binaries", file=sys.stderr)
        return 2
    with tempfile.TemporaryDirectory(prefix="nonodb-graph-auth-") as td:
        tmp = Path(td)
        auth_file = tmp / "auth.json"
        run_cli(auth_file, "init")
        admin = run_cli(auth_file, "token", "create", "admin")
        port = free_port()
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(tmp / "data"),
            NONODB_SOCK=str(tmp / "n.sock"),
            NONODB_TCP=f"127.0.0.1:{port}",
            NONODB_AUTH_FILE=str(auth_file),
            NONODB_LOG="warn",
        )
        proc = subprocess.Popen([str(daemon)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            base = f"http://127.0.0.1:{port}"
            deadline = time.monotonic() + 15
            with httpx.Client(base_url=base, timeout=5, trust_env=False) as client:
                while time.monotonic() < deadline:
                    try:
                        if client.get("/healthz").status_code == 200:
                            break
                    except httpx.HTTPError:
                        time.sleep(0.05)
                else:
                    raise RuntimeError("daemon did not become healthy")

                def call(token: str, payload: dict) -> httpx.Response:
                    return client.post(
                        "/v1/exec",
                        headers={"authorization": f"Bearer {token}"},
                        json=payload,
                    )

                for coll in ("nodes", "edges"):
                    response = call(admin, {"op": "create_collection", "coll": coll})
                    assert response.status_code == 200, response.text
                response = call(
                    admin,
                    {"op": "create_collection", "coll": "vaulted_edges", "profile": "vault"},
                )
                assert response.status_code == 200, response.text
                for doc in ({"_id": "a"}, {"_id": "b"}):
                    assert call(admin, {"op": "insert", "coll": "nodes", "doc": doc}).status_code == 200
                assert call(
                    admin,
                    {"op": "insert", "coll": "edges", "doc": {"_id": "e1", "from": "a", "to": "b"}},
                ).status_code == 200
                assert call(
                    admin,
                    {
                        "op": "set_graph",
                        "coll": "nodes",
                        "def": {"node_collection": "nodes", "edge_collection": "edges"},
                    },
                ).status_code == 200

                vault = call(
                    admin,
                    {
                        "op": "set_graph",
                        "coll": "nodes",
                        "def": {"node_collection": "nodes", "edge_collection": "vaulted_edges"},
                    },
                )
                assert vault.status_code == 400, vault.text
                assert "vault" in vault.text.lower(), vault.text

                def principal(name: str, scopes: list[str], kid: str) -> str:
                    response = client.post(
                        "/v1/auth/principals",
                        headers={"authorization": f"Bearer {admin}"},
                        json={"name": name, "scopes": scopes},
                    )
                    assert response.status_code == 200, response.text
                    response = client.post(
                        "/v1/auth/tokens",
                        headers={"authorization": f"Bearer {admin}"},
                        json={"principal": name, "kid": kid},
                    )
                    assert response.status_code == 200, response.text
                    return response.json()["result"]["token"]

                node_only = principal("node_only", ["read:nodes"], "nodeonly1")
                edge_only = principal("edge_only", ["read:edges"], "edgeonly1")
                graph_reader = principal("graph_reader", ["read:nodes", "read:edges"], "graph1")

                for payload in (
                    {"op": "get_graph", "coll": "nodes"},
                    {"op": "traverse", "coll": "nodes", "start": ["a"]},
                    {"op": "shortest_path", "coll": "nodes", "start": "a", "target": "b"},
                ):
                    denied = call(node_only, payload)
                    assert denied.status_code == 403, (payload, denied.status_code, denied.text)
                    denied = call(edge_only, payload)
                    assert denied.status_code == 403, (payload, denied.status_code, denied.text)
                    allowed = call(graph_reader, payload)
                    assert allowed.status_code == 200, (payload, allowed.status_code, allowed.text)
                print("graph auth tests passed")
                return 0
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
