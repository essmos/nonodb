"""End-to-end per-database quotas: API, CLI, hard rejection, and restart.

    cargo build --locked --bins
    python3 scripts/quota_test.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB, NonoDBError  # noqa: E402

DAEMON = Path(os.environ.get("NONODB_BIN", ROOT / "target/debug/nonodb-daemon"))
CLI = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))
DATABASE = "quota_test"


def wait_socket(path: Path, proc: subprocess.Popen[bytes], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError("daemon exited before creating its Unix socket")
        time.sleep(0.05)
    raise RuntimeError("timed out waiting for the daemon socket")


def start_daemon(tmp: Path) -> tuple[subprocess.Popen[bytes], Path, Path]:
    data = tmp / "data"
    sock = tmp / "nonodb.sock"
    log = (tmp / "daemon.log").open("ab")
    env = os.environ.copy()
    env.update({"NONODB_DATA": str(data), "NONODB_SOCK": str(sock), "NONODB_LOG": "warn"})
    proc = subprocess.Popen(
        [str(DAEMON)],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    log.close()
    wait_socket(sock, proc)
    return proc, data, sock


def stop_daemon(proc: subprocess.Popen[bytes]) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def all_limits(**values: int | None) -> dict[str, int | None]:
    limits = {
        "data_bytes": None,
        "index_bytes": None,
        "history_bytes": None,
        "sidecar_bytes": None,
    }
    limits.update(values)
    return limits


def expect_quota_error(action, label: str) -> None:
    try:
        action()
    except NonoDBError as error:
        if error.code != "quota_exceeded":
            raise AssertionError(f"{label}: expected quota_exceeded, got {error.code}: {error}")
    else:
        raise AssertionError(f"{label}: write was not rejected")


def run_cli(sock: Path, *args: str) -> dict:
    env = os.environ.copy()
    env["NONODB_SOCK"] = str(sock)
    result = subprocess.run(
        [str(CLI), "--db", DATABASE, *args],
        env=env,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise AssertionError(f"CLI failed: {result.stderr or result.stdout}")
    return json.loads(result.stdout)


def main() -> int:
    if not DAEMON.is_file():
        raise SystemExit(f"missing daemon binary: {DAEMON}")
    if not CLI.is_file():
        raise SystemExit(f"missing CLI binary: {CLI}")
    with tempfile.TemporaryDirectory(prefix="nonodb-quota-") as raw:
        tmp = Path(raw)
        proc, data, sock = start_daemon(tmp)
        try:
            root = NonoDB(socket_path=str(sock))
            root.exec({"op": "create_database", "name": DATABASE})
            db = NonoDB(socket_path=str(sock), database=DATABASE)
            db.create_collection("docs")
            docs = db.collection("docs")
            docs.insert({"_id": "seed", "text": "seed" * 32})

            report = db.exec({"op": "get_quota"})
            assert report["database"] == DATABASE, report
            assert report["limits"] == {}, report
            assert report["status"] == "ok", report
            usage = report["usage"]
            for field in ("data_bytes", "index_bytes", "history_bytes", "sidecar_bytes"):
                assert isinstance(usage[field], int), report

            # Data: cross the soft threshold, then reject a growing write.
            data_limit = usage["data_bytes"] + 1
            warned = db.exec(
                {"op": "set_quota", "limits": all_limits(data_bytes=data_limit)}
            )
            assert warned["status"] == "warning", warned
            assert warned["warnings"][0]["resource"] == "data", warned
            expect_quota_error(
                lambda: docs.insert({"_id": "blocked-data", "text": "x" * 800}),
                "data quota",
            )
            assert docs.delete({"_id": "blocked-data"}) == 0
            docs.delete({"_id": "seed"})
            assert docs.count() == 0

            docs.insert({"_id": "seed", "text": "seed" * 32})
            db.exec(
                {
                    "op": "set_quota",
                    "limits": all_limits(index_bytes=usage["index_bytes"] + 1),
                }
            )
            expect_quota_error(
                lambda: docs.create_index("text", unique=False),
                "index quota",
            )

            # History: enable revisions first, then block the update.
            db.exec({"op": "set_quota", "limits": all_limits()})
            db.exec({"op": "set_collection_history", "coll": "docs", "max_versions": 3})
            db.exec(
                {
                    "op": "set_quota",
                    "limits": all_limits(history_bytes=usage["history_bytes"] + 1),
                }
            )
            expect_quota_error(
                lambda: docs.update({"_id": "seed"}, {"$set": {"text": "changed"}}),
                "history quota",
            )

            # Sidecar: creating a text index must be rejected before files are
            # written when the sidecar budget is exhausted.
            db.exec({"op": "set_quota", "limits": all_limits()})
            db.exec(
                {
                    "op": "set_quota",
                    "limits": all_limits(sidecar_bytes=usage["sidecar_bytes"] + 1),
                }
            )
            expect_quota_error(lambda: docs.create_text_index("text"), "sidecar quota")

            # Clear limits: writes work again and the persisted file records the
            # reset.
            cleared = db.exec({"op": "set_quota", "limits": all_limits()})
            assert cleared["status"] == "ok", cleared
            docs.insert({"_id": "after-clear", "text": "ok"})
            assert docs.find_one({"_id": "after-clear"}) is not None

            # CLI reads and updates the persisted limits.
            cli_report = run_cli(sock, "quota")
            assert cli_report["database"] == DATABASE, cli_report
            cli_set = run_cli(sock, "quota", "set", "--data", "1GiB")
            assert cli_set["limits"]["data_bytes"] == 1 << 30, cli_set
            cli_reset = run_cli(sock, "quota", "reset")
            assert cli_reset["limits"] == {}, cli_reset

            # Persist a limit across a daemon restart and enforce it.
            db.exec(
                {
                    "op": "set_quota",
                    "limits": all_limits(data_bytes=usage["data_bytes"] + 1),
                }
            )
            stop_daemon(proc)
            proc, data, sock = start_daemon(tmp)
            db = NonoDB(socket_path=str(sock), database=DATABASE)
            docs = db.collection("docs")
            persisted = db.exec({"op": "get_quota"})
            assert persisted["limits"]["data_bytes"] == usage["data_bytes"] + 1, persisted
            assert persisted["status"] == "warning", persisted
            expect_quota_error(
                lambda: docs.insert({"_id": "after-restart", "text": "blocked"}),
                "persisted quota",
            )
            assert db.exec({"op": "database_stats"})["quota"]["status"] == "warning"
        finally:
            stop_daemon(proc)

    print("quota test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
