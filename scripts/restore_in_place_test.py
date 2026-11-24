"""End-to-end verified restore-in-place through the admin HTTP API.

    cargo build --locked --bins
    python3 scripts/restore_in_place_test.py
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

from nonodb import NonoDB  # noqa: E402

DAEMON = Path(os.environ.get("NONODB_BIN", ROOT / "target/debug/nonodb-daemon"))
CLI = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))
DATABASE = "restore_src"


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
    env.update(
        {
            "NONODB_DATA": str(data),
            "NONODB_SOCK": str(sock),
            "NONODB_LOG": "warn",
            "NONODB_IMPORT_ROOTS": str(tmp / "backups"),
            "NONODB_AUDIT_LOG": str(tmp / "audit.log"),
        }
    )
    proc = subprocess.Popen([str(DAEMON)], env=env, stdout=log, stderr=subprocess.STDOUT)
    log.close()
    wait_socket(sock, proc)
    return proc, data, sock


def stop_daemon(proc: subprocess.Popen[bytes]) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def api(client: NonoDB, method: str, path: str, payload: dict) -> tuple[int, dict]:
    response = client._http.request(method, path, json=payload)
    try:
        body = response.json()
    except Exception:
        body = {"raw": response.text}
    return response.status_code, body


def create_backup(tmp: Path, data: Path) -> Path:
    out = tmp / "backups" / DATABASE
    result = subprocess.run(
        [str(CLI), "--db", DATABASE, "backup", str(data), str(out)],
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise AssertionError(f"backup failed: {result.stderr or result.stdout}")
    assert (out / "manifest.json").is_file()
    return out


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit(f"missing binaries: {DAEMON}, {CLI}")
    with tempfile.TemporaryDirectory(prefix="nonodb-restore-") as raw:
        tmp = Path(raw)
        proc, data, sock = start_daemon(tmp)
        try:
            root = NonoDB(socket_path=str(sock))
            root.exec({"op": "create_database", "name": DATABASE})
            db = NonoDB(socket_path=str(sock), database=DATABASE)
            db.create_collection("docs")
            db.collection("docs").insert({"_id": "a", "phase": "backup"})
        finally:
            stop_daemon(proc)

        backup = create_backup(tmp, data)

        # A tampered copy must fail preflight before anything is changed.
        tampered = tmp / "backups" / "tampered"
        shutil.copytree(backup, tampered)
        manifest_path = tampered / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        with (tampered / manifest["data_file"]).open("ab") as handle:
            handle.write(b"tampered")
        proc, data, sock = start_daemon(tmp)
        try:
            db = NonoDB(socket_path=str(sock), database=DATABASE)
            db.collection("docs").insert({"_id": "b", "phase": "live"})
            status, body = api(
                db,
                "POST",
                "/v1/restore/preflight",
                {"source": {"kind": "local", "path": str(tampered)}, "location": str(tampered), "database": DATABASE},
            )
            assert status == 400, (status, body)
            assert db.collection("docs").count() == 2

            status, body = api(
                db,
                "POST",
                "/v1/restore/preflight",
                {"source": {"kind": "local", "path": str(backup)}, "location": str(backup), "database": DATABASE},
            )
            assert status == 200, (status, body)
            preflight = body["result"]
            assert preflight["database"] == DATABASE, body
            assert preflight["confirmation"] == f"RESTORE {DATABASE}", body
            assert preflight["manifest"]["sha256"] == manifest["sha256"], body

            # Typed confirmation is mandatory.
            status, body = api(
                db,
                "POST",
                "/v1/restore",
                {
                    "source": {"kind": "local", "path": str(backup)},
                    "location": str(backup),
                    "database": DATABASE,
                    "confirmation": "wrong",
                },
            )
            assert status == 400, (status, body)
            assert db.collection("docs").count() == 2

            status, body = api(
                db,
                "POST",
                "/v1/restore",
                {
                    "source": {"kind": "local", "path": str(backup)},
                    "location": str(backup),
                    "database": DATABASE,
                    "confirmation": preflight["confirmation"],
                },
            )
            assert status == 200, (status, body)
            result = body["result"]
            assert result["database"] == DATABASE, body
            assert result["backup"] == str(backup), body
            safety = Path(result["safety_snapshot"])
            assert safety.is_dir(), result
            assert (safety / "data.mdb").is_file(), result
            assert (safety / "restore-info.json").is_file(), result
            info = json.loads((safety / "restore-info.json").read_text())
            assert info["database"] == DATABASE, info
            assert "rollback" in result and result["rollback"], result

            # The live database now matches the backup, not the later insert.
            docs = db.collection("docs")
            assert docs.count() == 1
            assert docs.find_one({"_id": "a"})["phase"] == "backup"
            assert docs.find_one({"_id": "b"}) is None

            audit = [
                json.loads(line)
                for line in (tmp / "audit.log").read_text().splitlines()
                if line.strip()
            ]
            succeeded = [
                entry
                for entry in audit
                if entry.get("op") == "restore_in_place"
                and entry.get("result") == "ok"
            ]
            assert succeeded, audit
            assert succeeded[-1]["details"]["database"] == DATABASE, succeeded[-1]
            assert succeeded[-1]["details"]["location"] == str(backup), succeeded[-1]
            assert any(
                entry.get("op") == "restore_preflight" and entry.get("result") == "ok"
                for entry in audit
            ), audit
        finally:
            stop_daemon(proc)

    print("restore in place test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
