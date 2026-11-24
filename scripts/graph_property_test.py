#!/usr/bin/env python3
"""Randomized adjacency consistency check against a Python BFS oracle."""

from __future__ import annotations

import os
import random
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))
from nonodb import NonoDB  # noqa: E402


def reachable(edges: list[dict], start: str, max_depth: int) -> dict[str, int]:
    graph: dict[str, list[str]] = {}
    for edge in edges:
        graph.setdefault(str(edge["from"]), []).append(str(edge["to"]))
    depth = {start: 0}
    frontier = [start]
    for at in range(max_depth):
        nxt = []
        for node in frontier:
            for target in graph.get(node, []):
                if target not in depth:
                    depth[target] = at + 1
                    nxt.append(target)
        frontier = nxt
    return depth


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.is_file():
        raise SystemExit("missing debug daemon binary")
    with tempfile.TemporaryDirectory(prefix="nonodb-graph-prop-") as td:
        tmp = Path(td)
        sock = tmp / "n.sock"
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(tmp / "data"),
            NONODB_SOCK=str(sock),
            NONODB_LOG="warn",
        )
        proc = subprocess.Popen([str(daemon)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 10
            while not sock.exists() and time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError("daemon exited")
                time.sleep(0.02)
            db = NonoDB(str(sock), cache_ttl=0.0)
            nodes = db.collection("nodes")
            db.create_collection("edges")
            edges = db.collection("edges")
            nodes.set_graph("edges")
            nodes.insert_many([{"_id": f"n{i}"} for i in range(30)])
            oracle: list[dict] = []
            next_id = 0
            for seed in range(12):
                rng = random.Random(seed)
                for _ in range(rng.randint(20, 45)):
                    edge = {
                        "_id": f"e{next_id}",
                        "from": f"n{rng.randrange(30)}",
                        "to": f"n{rng.randrange(30)}",
                        "label": rng.choice(["A", "B", "C"]),
                    }
                    next_id += 1
                    edges.insert(edge)
                    oracle.append(edge)
                # Mutate a mix of live edges while keeping the oracle exact.
                for edge in rng.sample(oracle, k=min(8, len(oracle))):
                    if rng.random() < 0.5:
                        live = edges.find_one({"_id": edge["_id"]})
                        if live is None:
                            continue
                        target = f"n{rng.randrange(30)}"
                        edges.update({"_id": edge["_id"]}, {"$set": {"to": target}})
                        edge["to"] = target
                    else:
                        if edges.delete({"_id": edge["_id"]}, multi=False):
                            oracle.remove(edge)
                for start in [f"n{i}" for i in rng.sample(range(30), 5)]:
                    result = nodes.traverse(
                        start,
                        max_depth=3,
                        tree=False,
                        limit_nodes=10_000,
                        limit_edges=100_000,
                    )
                    got = {
                        str(item["node"]["_id"]): item["depth"]
                        for item in result["items"]
                        if not item["cycle"]
                    }
                    expected = reachable(oracle, start, 3)
                    assert got == expected, (seed, start, got, expected)
                for start, target in rng.sample(
                    [(f"n{i}", f"n{j}") for i in range(30) for j in range(30)], 8
                ):
                    expected = reachable(oracle, start, 4)
                    path = nodes.shortest_path(start, target, max_depth=4)
                    if target in expected:
                        assert path["found"] is True, (seed, start, target, path)
                        assert path["hops"] == expected[target], (seed, start, target, path)
                    else:
                        assert path["found"] is False, (seed, start, target, path)
            print("graph property tests passed")
            return 0
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
