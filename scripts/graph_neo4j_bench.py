#!/usr/bin/env python3
"""Benchmark NonoDB graph traversal against Neo4j Community.

This is an intentionally small, reproducible comparison (2k nodes / 8k edges)
on one machine. It measures end-to-end HTTP/socket request latency after a
warmup and reports medians, not a scientific graph-database benchmark.

Neo4j Community has no built-in multi-node replication; this script compares a
single Neo4j instance with a single NonoDB daemon. Run it on a quiet machine and
report both absolute and relative numbers.

Usage:
    cargo build --release --bins
    docker pull neo4j:5-community
    python3 scripts/graph_neo4j_bench.py

Environment:
    NONODB_BENCH_NODES / NONODB_BENCH_EDGES
    NONODB_DAEMON (defaults to target/release/nonodb-daemon)
    NEO4J_IMAGE (default neo4j:5-community)
"""

from __future__ import annotations

import json
import os
import random
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from collections import deque
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))
from nonodb import NonoDB  # noqa: E402

PASSWORD = "benchpass123"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def pct(values: list[float], q: float) -> float:
    values = sorted(values)
    return values[min(len(values) - 1, max(0, int(len(values) * q)))]


def stats(values: list[float]) -> dict[str, float]:
    return {
        "p50_ms": round(statistics.median(values), 3),
        "p95_ms": round(pct(values, 0.95), 3),
        "min_ms": round(min(values), 3),
    }


def wait_socket(path: Path, proc: subprocess.Popen[bytes], timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists() and time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("NonoDB daemon exited during startup")
        time.sleep(0.02)
    if not path.exists():
        raise RuntimeError("timed out waiting for NonoDB socket")


class Neo4j:
    def __init__(self, port: int) -> None:
        self.client = httpx.Client(
            base_url=f"http://127.0.0.1:{port}",
            auth=("neo4j", PASSWORD),
            timeout=120.0,
            trust_env=False,
        )

    def close(self) -> None:
        self.client.close()

    def cypher(self, statement: str, parameters: dict | None = None) -> list[dict]:
        response = self.client.post(
            "/db/neo4j/tx/commit",
            json={"statements": [{"statement": statement, "parameters": parameters or {}}]},
        )
        response.raise_for_status()
        body = response.json()
        if body.get("errors"):
            raise RuntimeError(body["errors"])
        result = body["results"][0]
        return result.get("data", [])

    def scalar(self, statement: str, parameters: dict | None = None):
        data = self.cypher(statement, parameters)
        if not data:
            return None
        return data[0]["row"][0]

    def wait_ready(self, timeout: float = 120) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if self.scalar("RETURN 1") == 1:
                    return
            except (httpx.HTTPError, RuntimeError, KeyError):
                pass
            time.sleep(0.5)
        raise RuntimeError("timed out waiting for Neo4j")


def batches(items: list[dict], size: int):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def load_neo4j(neo: Neo4j, nodes: list[dict], edges: list[dict], batch: int) -> float:
    start = time.perf_counter()
    for chunk in batches(nodes, batch):
        rows = [{"id": node["_id"]} for node in chunk]
        neo.cypher("UNWIND $rows AS row CREATE (:Node {id: row.id})", {"rows": rows})
    for chunk in batches(edges, batch):
        rows = [
            {"id": edge["_id"], "src": edge["from"], "dst": edge["to"], "label": edge["label"]}
            for edge in chunk
        ]
        neo.cypher(
            "UNWIND $rows AS row "
            "MATCH (a:Node {id: row.src}), (b:Node {id: row.dst}) "
            "CREATE (a)-[:EDGE {id: row.id, label: row.label}]->(b)",
            {"rows": rows},
        )
    return (time.perf_counter() - start) * 1000


def load_nonodb(db: NonoDB, nodes: list[dict], edges: list[dict], batch: int) -> float:
    db.create_collection("nodes")
    db.create_collection("edges")
    coll = db.collection("nodes")
    edge_coll = db.collection("edges")
    coll.set_graph("edges")
    start = time.perf_counter()
    for chunk in batches(nodes, batch):
        coll.insert_many(chunk)
    for chunk in batches(edges, batch):
        edge_coll.insert_many(chunk)
    return (time.perf_counter() - start) * 1000


def bfs_pairs(edges: list[dict], roots: list[str], count: int) -> list[tuple[str, str]]:
    graph: dict[str, list[str]] = {}
    for edge in edges:
        graph.setdefault(edge["from"], []).append(edge["to"])
    pairs: list[tuple[str, str]] = []
    for root in roots:
        depth = {root: 0}
        queue = deque([root])
        while queue and len(pairs) < count:
            node = queue.popleft()
            if depth[node] >= 4:
                continue
            for target in graph.get(node, []):
                if target in depth:
                    continue
                depth[target] = depth[node] + 1
                queue.append(target)
                if depth[target] >= 2:
                    pairs.append((root, target))
                    if len(pairs) == count:
                        return pairs
    return pairs


def main() -> int:
    release = ROOT / "target/release/nonodb-daemon"
    daemon = Path(os.environ.get("NONODB_DAEMON", release if release.is_file() else ROOT / "target/debug/nonodb-daemon"))
    if not daemon.is_file():
        raise SystemExit("missing daemon binary; run cargo build --release --bins")
    image = os.environ.get("NEO4J_IMAGE", "neo4j:5-community")
    n = int(os.environ.get("NONODB_BENCH_NODES", "2000"))
    m = int(os.environ.get("NONODB_BENCH_EDGES", "8000"))
    batch = int(os.environ.get("NONODB_BENCH_BATCH", "1000"))
    rng = random.Random(42)
    node_docs = [{"_id": f"n{i}", "id": f"n{i}", "_labels": ["Node"]} for i in range(n)]
    edges = [
        {
            "_id": f"e{i}",
            "from": f"n{i}",
            "to": f"n{(i + 1) % n}",
            "label": "RING",
        }
        for i in range(n)
    ]
    for i in range(n, m):
        edges.append(
            {
                "_id": f"e{i}",
                "from": f"n{rng.randrange(n)}",
                "to": f"n{rng.randrange(n)}",
                "label": rng.choice(["A", "B", "C"]),
            }
        )
    starts = [f"n{rng.randrange(n)}" for _ in range(20)]
    pairs = bfs_pairs(edges, starts, 20)
    if not pairs:
        raise RuntimeError("dataset has no reachable shortest-path pairs")

    temp = Path(
        tempfile.mkdtemp(
            prefix="nonodb-neo4j-bench-",
            dir=os.environ.get("NONODB_BENCH_DIR", "/var/tmp"),
        )
    )
    sock = temp / "n.sock"
    env = os.environ.copy()
    env.update(NONODB_DATA=str(temp / "data"), NONODB_SOCK=str(sock), NONODB_LOG="warn")
    nonodb_proc: subprocess.Popen[bytes] | None = None
    neo4j_proc: subprocess.Popen[bytes] | None = None
    container = f"nonodb-neo4j-bench-{os.getpid()}"
    port = free_port()
    neo: Neo4j | None = None
    try:
        nonodb_proc = subprocess.Popen(
            [str(daemon)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        wait_socket(sock, nonodb_proc)
        db = NonoDB(str(sock), cache_ttl=0.0)

        neo4j_proc = subprocess.Popen(
            [
                "docker",
                "run",
                "--rm",
                "-d",
                "--name",
                container,
                "-p",
                f"127.0.0.1:{port}:7474",
                "-e",
                f"NEO4J_AUTH=neo4j/{PASSWORD}",
                image,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        neo = Neo4j(port)
        neo.wait_ready()
        neo.cypher(
            "CREATE CONSTRAINT node_id IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE"
        )

        nonodb_ms = load_nonodb(db, node_docs, edges, batch)
        neo4j_ms = load_neo4j(neo, node_docs, edges, batch)
        coll = db.collection("nodes")

        # Warm both engines before timing request latency.
        for _ in range(3):
            for start in starts[:5]:
                coll.traverse(start, max_depth=3, tree=False, projection={"_id": 1})
                neo.scalar(
                    "MATCH (n:Node {id: $id})-[:EDGE*1..3]->(m) "
                    "RETURN count(DISTINCT m.id)",
                    {"id": start},
                )
                coll.shortest_path(pairs[0][0], pairs[0][1], max_depth=6, projection={"_id": 1})
                neo.scalar(
                    "MATCH (a:Node {id: $a}), (b:Node {id: $b}) "
                    "MATCH p=shortestPath((a)-[:EDGE*..6]->(b)) RETURN length(p)",
                    {"a": pairs[0][0], "b": pairs[0][1]},
                )
                coll.all_shortest_paths(
                    pairs[0][0],
                    pairs[0][1],
                    max_depth=6,
                    limit_paths=10,
                    projection={"_id": 1},
                )
                neo.scalar(
                    "MATCH (a:Node {id: $a}), (b:Node {id: $b}) "
                    "MATCH p=allShortestPaths((a)-[:EDGE*..6]->(b)) RETURN count(p)",
                    {"a": pairs[0][0], "b": pairs[0][1]},
                )
                coll.graph_match(
                    start,
                    [{"direction": "out", "min_hops": 1, "max_hops": 2}],
                    limit=100,
                    projection={"_id": 1},
                )
                neo.scalar(
                    "MATCH p=(n:Node {id: $id})-[:EDGE*1..2]->(m) RETURN count(p)",
                    {"id": start},
                )
                coll.cypher(
                    "MATCH p=(a:Node)-[*1..2]->(b) RETURN count(p) AS c",
                    max_rows=10,
                )
                neo.scalar("MATCH p=(a:Node)-[*1..2]->(b) RETURN count(p)")
                coll.cypher(
                    "MATCH (a:Node {id: $a}), (b:Node {id: $b}) "
                    "MATCH p=shortestPath((a)-[*..6]->(b)) RETURN length(p) AS len",
                    params={"a": pairs[0][0], "b": pairs[0][1]},
                    max_rows=10,
                )
                neo.scalar(
                    "MATCH (a:Node {id: $a}), (b:Node {id: $b}) "
                    "MATCH p=shortestPath((a)-[*..6]->(b)) RETURN length(p)",
                    {"a": pairs[0][0], "b": pairs[0][1]},
                )

        def time_nonodb_traverse(depth: int) -> list[float]:
            values = []
            for _ in range(5):
                for start in starts:
                    t0 = time.perf_counter()
                    coll.traverse(start, max_depth=depth, tree=False, projection={"_id": 1})
                    values.append((time.perf_counter() - t0) * 1000)
            return values

        def time_neo4j_traverse(depth: int) -> list[float]:
            values = []
            pattern = "-[:EDGE]->" if depth == 1 else f"-[:EDGE*1..{depth}]->"
            for _ in range(5):
                for start in starts:
                    t0 = time.perf_counter()
                    neo.scalar(
                        f"MATCH (n:Node {{id: $id}}){pattern}(m) "
                        "RETURN count(DISTINCT m.id)",
                        {"id": start},
                    )
                    values.append((time.perf_counter() - t0) * 1000)
            return values

        def time_nonodb_shortest() -> list[float]:
            values = []
            for _ in range(5):
                for start, target in pairs:
                    t0 = time.perf_counter()
                    coll.shortest_path(start, target, max_depth=6, projection={"_id": 1})
                    values.append((time.perf_counter() - t0) * 1000)
            return values

        def time_neo4j_shortest() -> list[float]:
            values = []
            for _ in range(5):
                for start, target in pairs:
                    t0 = time.perf_counter()
                    neo.scalar(
                        "MATCH (a:Node {id: $a}), (b:Node {id: $b}) "
                        "MATCH p=shortestPath((a)-[:EDGE*..6]->(b)) RETURN length(p)",
                        {"a": start, "b": target},
                    )
                    values.append((time.perf_counter() - t0) * 1000)
            return values

        def time_nonodb_all_shortest() -> list[float]:
            values = []
            for _ in range(5):
                for start, target in pairs:
                    t0 = time.perf_counter()
                    coll.all_shortest_paths(
                        start,
                        target,
                        max_depth=6,
                        limit_paths=10,
                        projection={"_id": 1},
                    )
                    values.append((time.perf_counter() - t0) * 1000)
            return values

        def time_neo4j_all_shortest() -> list[float]:
            values = []
            for _ in range(5):
                for start, target in pairs:
                    t0 = time.perf_counter()
                    neo.scalar(
                        "MATCH (a:Node {id: $a}), (b:Node {id: $b}) "
                        "MATCH p=allShortestPaths((a)-[:EDGE*..6]->(b)) RETURN count(p)",
                        {"a": start, "b": target},
                    )
                    values.append((time.perf_counter() - t0) * 1000)
            return values

        def time_nonodb_match() -> list[float]:
            values = []
            for _ in range(5):
                for start in starts:
                    t0 = time.perf_counter()
                    coll.graph_match(
                        start,
                        [{"direction": "out", "min_hops": 1, "max_hops": 2}],
                        limit=100,
                        projection={"_id": 1},
                    )
                    values.append((time.perf_counter() - t0) * 1000)
            return values

        def time_neo4j_match() -> list[float]:
            values = []
            for _ in range(5):
                for start in starts:
                    t0 = time.perf_counter()
                    neo.scalar(
                        "MATCH p=(n:Node {id: $id})-[:EDGE*1..2]->(m) RETURN count(p)",
                        {"id": start},
                    )
                    values.append((time.perf_counter() - t0) * 1000)
            return values

        def time_nonodb_cypher_match() -> list[float]:
            values = []
            for _ in range(5):
                t0 = time.perf_counter()
                coll.cypher(
                    "MATCH p=(a:Node)-[*1..2]->(b) RETURN count(p) AS c",
                    max_rows=10,
                )
                values.append((time.perf_counter() - t0) * 1000)
            return values

        def time_neo4j_cypher_match() -> list[float]:
            values = []
            for _ in range(5):
                t0 = time.perf_counter()
                neo.scalar("MATCH p=(a:Node)-[*1..2]->(b) RETURN count(p)")
                values.append((time.perf_counter() - t0) * 1000)
            return values

        def time_nonodb_cypher_shortest() -> list[float]:
            values = []
            for _ in range(5):
                for start, target in pairs:
                    t0 = time.perf_counter()
                    coll.cypher(
                        "MATCH (a:Node {id: $a}), (b:Node {id: $b}) "
                        "MATCH p=shortestPath((a)-[*..6]->(b)) RETURN length(p) AS len",
                        params={"a": start, "b": target},
                        max_rows=10,
                    )
                    values.append((time.perf_counter() - t0) * 1000)
            return values

        def time_neo4j_cypher_shortest() -> list[float]:
            values = []
            for _ in range(5):
                for start, target in pairs:
                    t0 = time.perf_counter()
                    neo.scalar(
                        "MATCH (a:Node {id: $a}), (b:Node {id: $b}) "
                        "MATCH p=shortestPath((a)-[*..6]->(b)) RETURN length(p)",
                        {"a": start, "b": target},
                    )
                    values.append((time.perf_counter() - t0) * 1000)
            return values

        # Validate reachability counts for one start on both engines.
        sample = starts[0]
        nonodb_sample = coll.traverse(
            sample, max_depth=3, tree=False, projection={"_id": 1}
        )
        nonodb_ids = {
            item["node"]["_id"] for item in nonodb_sample["items"] if not item["cycle"]
        }
        neo4j_count = neo.scalar(
            "MATCH (n:Node {id: $id})-[:EDGE*1..3]->(m) RETURN count(DISTINCT m.id)",
            {"id": sample},
        )
        assert len(nonodb_ids) - 1 == neo4j_count, (len(nonodb_ids) - 1, neo4j_count)

        all_sample = coll.all_shortest_paths(
            pairs[0][0],
            pairs[0][1],
            max_depth=6,
            limit_paths=100,
            projection={"_id": 1},
        )
        neo4j_all_count = neo.scalar(
            "MATCH (a:Node {id: $a}), (b:Node {id: $b}) "
            "MATCH p=allShortestPaths((a)-[:EDGE*..6]->(b)) RETURN count(p)",
            {"a": pairs[0][0], "b": pairs[0][1]},
        )
        assert all_sample["path_count"] == neo4j_all_count, (
            all_sample["path_count"],
            neo4j_all_count,
        )

        match_sample = coll.graph_match(
            sample,
            [{"direction": "out", "min_hops": 1, "max_hops": 2}],
            limit=1000,
            projection={"_id": 1},
        )
        neo4j_match_count = neo.scalar(
            "MATCH p=(n:Node {id: $id})-[:EDGE*1..2]->(m) RETURN count(p)",
            {"id": sample},
        )
        assert match_sample["matches"] == neo4j_match_count, (
            match_sample["matches"],
            neo4j_match_count,
        )

        depth1_nonodb = stats(time_nonodb_traverse(1))
        depth1_neo4j = stats(time_neo4j_traverse(1))
        depth3_nonodb = stats(time_nonodb_traverse(3))
        depth3_neo4j = stats(time_neo4j_traverse(3))
        shortest_nonodb = stats(time_nonodb_shortest())
        shortest_neo4j = stats(time_neo4j_shortest())
        all_shortest_nonodb = stats(time_nonodb_all_shortest())
        all_shortest_neo4j = stats(time_neo4j_all_shortest())
        match_nonodb = stats(time_nonodb_match())
        match_neo4j = stats(time_neo4j_match())
        cypher_match_nonodb = stats(time_nonodb_cypher_match())
        cypher_match_neo4j = stats(time_neo4j_cypher_match())
        cypher_shortest_nonodb = stats(time_nonodb_cypher_shortest())
        cypher_shortest_neo4j = stats(time_neo4j_cypher_shortest())
        print(
            json.dumps(
                {
                    "dataset": {"nodes": n, "edges": m, "shortest_pairs": len(pairs)},
                    "ingest": {
                        "nonodb_ms": round(nonodb_ms, 3),
                        "neo4j_ms": round(neo4j_ms, 3),
                        "nonodb_edges_per_s": round(m / (nonodb_ms / 1000)),
                        "neo4j_edges_per_s": round(m / (neo4j_ms / 1000)),
                    },
                    "traverse_depth1": {"nonodb": depth1_nonodb, "neo4j": depth1_neo4j},
                    "traverse_depth3": {"nonodb": depth3_nonodb, "neo4j": depth3_neo4j},
                    "shortest_path_hops": {
                        "nonodb": shortest_nonodb,
                        "neo4j": shortest_neo4j,
                    },
                    "all_shortest_paths_hops": {
                        "nonodb": all_shortest_nonodb,
                        "neo4j": all_shortest_neo4j,
                    },
                    "graph_match_depth2": {
                        "nonodb": match_nonodb,
                        "neo4j": match_neo4j,
                    },
                    "cypher_match_depth2": {
                        "nonodb": cypher_match_nonodb,
                        "neo4j": cypher_match_neo4j,
                    },
                    "cypher_shortest_path": {
                        "nonodb": cypher_shortest_nonodb,
                        "neo4j": cypher_shortest_neo4j,
                    },
                    "queries": {
                        "cypher_match_depth2": "MATCH p=(a:Node)-[*1..2]->(b) RETURN count(p) AS c",
                        "cypher_shortest_path": "MATCH (a:Node {id: $a}), (b:Node {id: $b}) MATCH p=shortestPath((a)-[*..6]->(b)) RETURN length(p) AS len",
                    },
                    "notes": [
                        "End-to-end request latency after warmup, one machine.",
                        "NonoDB uses JSON-over-Unix-socket with document results; Neo4j uses HTTP transactional Cypher with count/ID results.",
                        "NonoDB ingest includes adjacency maintenance; Neo4j ingest includes relationship creation.",
                        "Neo4j Community has no built-in multi-node replication, so this is not a replication benchmark.",
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    finally:
        if neo is not None:
            neo.close()
        if neo4j_proc is not None:
            subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        if nonodb_proc is not None:
            nonodb_proc.terminate()
            try:
                nonodb_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                nonodb_proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
