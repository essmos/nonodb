"""Background graph-analytics jobs: progress, cancellation, pool, and results.

    cargo build -p nonodb-daemon
    python3 scripts/graph_jobs_test.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

NODES = 100_000


def wait_for_socket(path: Path, proc: subprocess.Popen[bytes], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError("daemon exited before creating its socket")
        time.sleep(0.05)
    raise RuntimeError("timed out waiting for daemon socket")


def wait_terminal(db: NonoDB, job_id: str, timeout: float = 20.0) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = db.exec({"op": "graph_job_status", "job_id": job_id})
        if last["state"] in {"completed", "failed", "cancelled"}:
            return last
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} did not finish: {last}")


def main() -> int:
    daemon = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
    if not daemon.exists():
        print(f"missing daemon binary: {daemon}", file=sys.stderr)
        return 2
    tmp = Path(tempfile.mkdtemp(prefix="nonodb-graph-jobs-"))
    sock = tmp / "nonodb.sock"
    log = tmp / "daemon.log"
    env = os.environ.copy()
    env.update(
        {
            "NONODB_DATA": str(tmp / "data"),
            "NONODB_SOCK": str(sock),
            "NONODB_LOG": "warn",
            "NONODB_REQUEST_TIMEOUT": "30",
        }
    )
    fh = log.open("wb")
    proc = subprocess.Popen([str(daemon)], env=env, stdout=fh, stderr=subprocess.STDOUT)
    fh.close()
    try:
        wait_for_socket(sock, proc)
        db = NonoDB(str(sock), cache_ttl=0.0)
        nodes = db.collection("nodes")
        edges = db.collection("edges")
        batch = []
        for i in range(NODES):
            batch.append({"_id": f"n{i:06}"})
            if len(batch) == 1000:
                nodes.insert_many(batch)
                batch.clear()
        if batch:
            nodes.insert_many(batch)
        batch = []
        for i in range(NODES - 1):
            batch.append(
                {"_id": f"e{i:06}", "from": f"n{i:06}", "to": f"n{i + 1:06}"}
            )
            if len(batch) == 1000:
                edges.insert_many(batch)
                batch.clear()
        if batch:
            edges.insert_many(batch)
        nodes.set_graph("edges")

        baseline_threads = len(os.listdir(f"/proc/{proc.pid}/task"))
        job_args = {
            "direction": "out",
            "max_nodes": NODES,
            "max_edges": NODES * 2,
            "iterations": 100,
            "limit": 10,
        }
        first = nodes.start_graph_job("pagerank", **job_args)
        second = nodes.start_graph_job("pagerank", **job_args)
        try:
            nodes.start_graph_job("pagerank", **job_args)
        except NonoDBError as err:
            assert "too_many_jobs" in str(err), err
        else:
            raise AssertionError("third concurrent job must be rejected")

        # The request loop must stay responsive while jobs run in the background.
        started = time.monotonic()
        status = nodes.graph_job_status(first["job_id"])
        assert time.monotonic() - started < 1.0, "status blocked behind analytics"
        assert status["state"] in {"queued", "running", "cancelling"}, status
        assert "phase" in status["progress"], status

        # Wait until the worker has reported real progress.
        deadline = time.monotonic() + 5.0
        saw_progress = False
        while time.monotonic() < deadline:
            status = nodes.graph_job_status(first["job_id"])
            if status["progress"]["phase"] in {"scan_nodes", "scan_edges", "pagerank"}:
                saw_progress = True
            if saw_progress:
                break
            time.sleep(0.005)
        assert saw_progress, status

        for job in (first, second):
            out = nodes.cancel_graph_job(job["job_id"])
            assert out["state"] in {"cancelling", "cancelled", "completed"}, out
        finals = [wait_terminal(db, job["job_id"]) for job in (first, second)]
        assert any(final["state"] == "cancelled" for final in finals), finals
        for final in finals:
            assert final["state"] in {"cancelled", "completed"}, final
            if final["state"] == "cancelled":
                assert final["error"]["code"] == "cancelled", final

        # A completed job retains its result for an explicit result read.
        small = nodes.start_graph_job("pagerank", **{**job_args, "iterations": 1})
        final = wait_terminal(db, small["job_id"])
        assert final["state"] == "completed", final
        assert final["result_available"] is True, final
        result = nodes.graph_job_result(small["job_id"])
        assert len(result["items"]) == 10, result
        assert all("id" in item and "score" in item for item in result["items"])
        time.sleep(0.2)
        assert len(os.listdir(f"/proc/{proc.pid}/task")) <= baseline_threads + 1, (
            "analytics worker threads leaked"
        )

        db.create_collection("rank_a")
        db.create_collection("rank_b")
        exported = nodes.export_graph_job(small["job_id"], "rank_a")
        assert exported["exported"] == 10, exported
        first_ids = sorted(doc["_id"] for doc in db.collection("rank_a").find({}))
        try:
            nodes.graph_job_result(small["job_id"])
        except NonoDBError as err:
            assert "not_found" in str(err), err
        else:
            raise AssertionError("exported result must not be returned again")

        second_job = nodes.start_graph_job("pagerank", **{**job_args, "iterations": 1})
        assert wait_terminal(db, second_job["job_id"])["state"] == "completed"
        nodes.export_graph_job(second_job["job_id"], "rank_b")
        second_ids = sorted(doc["_id"] for doc in db.collection("rank_b").find({}))
        assert first_ids == second_ids, (first_ids, second_ids)

        # Re-exporting into the same collection must abort atomically.
        third_job = nodes.start_graph_job("pagerank", **{**job_args, "iterations": 1})
        assert wait_terminal(db, third_job["job_id"])["state"] == "completed"
        try:
            nodes.export_graph_job(third_job["job_id"], "rank_a")
        except NonoDBError as err:
            assert "duplicate" in str(err).lower(), err
        else:
            raise AssertionError("duplicate export must fail")
        assert len(db.collection("rank_a").find({})) == 10

        try:
            db.exec({"op": "graph_job_result", "job_id": "job-missing"})
        except NonoDBError as err:
            assert "not_found" in str(err), err
        else:
            raise AssertionError("missing job must be reported explicitly")

        print("graph job tests passed")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
