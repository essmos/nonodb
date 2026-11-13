#!/usr/bin/env python3
"""End-to-end checks for scheduled backup jobs, age encryption, and S3 PUTs.

Starts an isolated daemon, creates local and S3-compatible jobs through the
admin API, runs them, verifies the encrypted local backup with `restore`, and
uses a fake S3 endpoint to check path-style uploads and SigV4 headers.

Usage:
    cargo build --locked --bins
    python3 scripts/backup_api_test.py
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx

ROOT = Path(__file__).resolve().parents[1]
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
CLI = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def cli_env(auth_file: Path, extra: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env["NONODB_AUTH_FILE"] = str(auth_file)
    if extra:
        env.update(extra)
    return env


def run_cli(auth_file: Path, *args: str, extra: dict[str, str] | None = None) -> str:
    proc = subprocess.run(
        [str(CLI), *args],
        env=cli_env(auth_file, extra),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"nonodb-cli {' '.join(args)}: {proc.stderr.strip()}")
    return proc.stdout.strip()


def wait_backup(client: httpx.Client, token: str, job_id: str, timeout: float = 30) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payload = client.get("/v1/backups", headers=headers).json()
        for item in payload["result"]["jobs"]:
            if item["job"]["id"] != job_id:
                continue
            state = item["state"]
            if state["last_status"] in {"ok", "error"} and not state["running"]:
                if state["last_status"] != "ok":
                    raise AssertionError(state)
                return item
        time.sleep(0.1)
    raise AssertionError(f"backup job {job_id} timed out")


def wait_import(client: httpx.Client, token: str, import_id: str, timeout: float = 30) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get(f"/v1/import/status/{import_id}", headers=headers)
        if response.status_code == 200:
            item = response.json()["result"]
            if item["status"] in {"ok", "error"}:
                if item["status"] != "ok":
                    raise AssertionError(item)
                return item
        time.sleep(0.1)
    raise AssertionError(f"import {import_id} timed out")


class FakeS3(BaseHTTPRequestHandler):
    received: list[dict] = []
    objects: dict[str, bytes] = {}

    def do_PUT(self) -> None:  # noqa: N802
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length)
        self.received.append(
            {
                "path": self.path,
                "authorization": self.headers.get("authorization", ""),
                "body": body,
            }
        )
        self.objects[self.path] = body
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        split = urlsplit(self.path)
        if "list-type" in parse_qs(split.query):
            query = parse_qs(split.query)
            prefix = query.get("prefix", [""])[0]
            bucket_prefix = split.path.lstrip("/").split("/", 1)[0]
            keys = []
            for path in sorted(self.objects):
                parts = path.lstrip("/").split("/", 1)
                if len(parts) != 2 or parts[0] != bucket_prefix:
                    continue
                key = parts[1]
                if key.startswith(prefix):
                    keys.append(key)
            xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                "<ListBucketResult>"
                f"<Prefix>{prefix}</Prefix>"
                "<IsTruncated>false</IsTruncated>"
                + "".join(f"<Contents><Key>{key}</Key></Contents>" for key in keys)
                + "</ListBucketResult>"
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(xml)))
            self.end_headers()
            self.wfile.write(xml)
            return
        body = self.objects.get(split.path)
        if body is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args) -> None:
        return


def main() -> int:
    if not DAEMON.is_file() or not CLI.is_file():
        raise SystemExit("missing debug binaries; run cargo build --locked --bins")

    temp = Path(tempfile.mkdtemp(prefix="nonodb-backup-api-", dir="/var/tmp"))
    data = temp / "data"
    data.mkdir()
    auth_file = temp / "auth.json"
    config = temp / "backups.json"
    local_root = temp / "backups"
    identity_file = temp / "backup-identity.txt"
    recipient_file = temp / "backup-recipient.txt"
    restic_binary = temp / "fake-restic"
    restic_log = temp / "restic.log"
    restic_password = temp / "restic-password"
    server: HTTPServer | None = None
    proc: subprocess.Popen | None = None
    log_file = None
    try:
        run_cli(auth_file, "auth", "init")
        admin = run_cli(auth_file, "auth", "token", "create", "admin")
        recipient = run_cli(auth_file, "backup-keygen", str(identity_file))
        recipient_file.write_text(recipient + "\n")
        restic_password.write_text("test-password\n")
        restic_binary.write_text(
            "#!/bin/sh\n"
            f"echo \"$*|$RESTIC_PASSWORD_FILE\" >> {restic_log}\n"
            "case \"$*\" in *'cat config'*) exit 1;; esac\n"
            "exit 0\n"
        )
        restic_binary.chmod(0o755)

        FakeS3.received = []
        FakeS3.objects = {}
        server = HTTPServer(("127.0.0.1", 0), FakeS3)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        s3_port = server.server_port
        daemon_port = free_port()
        base = f"http://127.0.0.1:{daemon_port}"
        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(data),
            NONODB_SOCK=str(temp / "nonodb.sock"),
            NONODB_TCP=f"127.0.0.1:{daemon_port}",
            NONODB_AUTH_FILE=str(auth_file),
            NONODB_BACKUP_CONFIG=str(config),
            NONODB_BACKUP_IDENTITY_FILE=str(identity_file),
            NONODB_IMPORT_ROOTS=str(temp),
            NONODB_LOG="warn",
            AWS_ACCESS_KEY_ID="test-access",
            AWS_SECRET_ACCESS_KEY="test-secret",
        )
        log_file = open(temp / "daemon.log", "w", encoding="utf-8")
        proc = subprocess.Popen(
            [str(DAEMON)], env=env, stdout=log_file, stderr=subprocess.STDOUT
        )
        headers = {"Authorization": f"Bearer {admin}"}

        with httpx.Client(base_url=base, timeout=30.0, trust_env=False) as client:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError(f"daemon exited with {proc.returncode}")
                try:
                    if client.get("/healthz").status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.05)
            else:
                raise RuntimeError("daemon did not become healthy")

            assert (
                client.post(
                    "/v1/exec",
                    headers=headers,
                    json={"op": "create_collection", "coll": "orders"},
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/v1/exec",
                    headers=headers,
                    json={"op": "insert", "coll": "orders", "doc": {"_id": "o1", "x": 1}},
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/v1/exec",
                    headers=headers,
                    json={"op": "insert", "coll": "secret", "doc": {"_id": "s1", "x": 2}},
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/v1/exec",
                    headers=headers,
                    json={"op": "create_database", "name": "app1"},
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/app1/v1/exec",
                    headers=headers,
                    json={"op": "insert", "coll": "orders", "doc": {"_id": "a1", "x": 3}},
                ).status_code
                == 200
            )

            local_job = {
                "id": "encrypted-local",
                "database": "default",
                "databases": ["default", "app1"],
                "enabled": True,
                "every_minutes": 1440,
                "destination": {"kind": "local", "path": str(local_root)},
                "encryption": {"kind": "age", "recipient_file": str(recipient_file)},
                "keep_last": 2,
                "verify": True,
                "compression": "gzip",
                "exclude_collections": ["secret"],
            }
            response = client.post("/v1/backups/jobs", headers=headers, json=local_job)
            assert response.status_code == 200, response.text
            assert client.post(
                "/v1/backups/jobs/encrypted-local/run", headers=headers
            ).status_code == 200
            wait_backup(client, admin, "encrypted-local")

            run_dir = sorted((local_root / "encrypted-local").iterdir())[-1]
            default_dir = run_dir / "default"
            app1_dir = run_dir / "app1"
            assert (default_dir / "data.mdb.gz.age").is_file()
            assert (app1_dir / "data.mdb.gz.age").is_file()
            manifest = json.loads((default_dir / "manifest.json").read_text())
            assert manifest["source"] == "default"
            assert manifest["encryption"]["kind"] == "age"
            assert manifest["compression"]["kind"] == "gzip"
            assert manifest["data_file"] == "data.mdb.gz.age"
            assert manifest["plain_size"] > 0 and manifest["plain_sha256"]
            app1_manifest = json.loads((app1_dir / "manifest.json").read_text())
            assert app1_manifest["source"] == "app1"
            info_text = (default_dir / "backup-info.txt").read_text()
            assert "Compression: gzip" in info_text
            assert "Encryption: age" in info_text
            assert "Excluded collections: secret" in info_text

            restored = temp / "restored"
            restore = subprocess.run(
                [str(CLI), "restore", str(default_dir), str(restored), "--force", "--verify"],
                env=cli_env(auth_file, {"NONODB_BACKUP_IDENTITY_FILE": str(identity_file)}),
                capture_output=True,
                text=True,
                check=False,
            )
            assert restore.returncode == 0, restore.stderr
            report = json.loads(run_cli(auth_file, "verify", str(restored), "--json"))
            assert "orders" in report["collections"], report["collections"]
            assert "secret" not in report["collections"], report["collections"]

            plain_gzip_root = temp / "plain-gzip"
            plain_gzip_job = {
                "id": "plain-gzip",
                "database": "default",
                "enabled": True,
                "every_minutes": 1440,
                "destination": {"kind": "local", "path": str(plain_gzip_root)},
                "keep_last": 2,
                "verify": True,
                "compression": "gzip",
            }
            assert (
                client.post("/v1/backups/jobs", headers=headers, json=plain_gzip_job).status_code
                == 200
            )
            assert client.post(
                "/v1/backups/jobs/plain-gzip/run", headers=headers
            ).status_code == 200
            wait_backup(client, admin, "plain-gzip")
            plain_gzip_dir = sorted((plain_gzip_root / "plain-gzip").iterdir())[-1]
            assert (plain_gzip_dir / "data.mdb.gz").is_file()
            plain_manifest = json.loads((plain_gzip_dir / "manifest.json").read_text())
            assert plain_manifest["compression"]["kind"] == "gzip"
            assert "encryption" not in plain_manifest
            checked = run_cli(auth_file, "verify-backup", str(plain_gzip_dir), "--json")
            assert json.loads(checked)["report"]["documents"] > 0
            restored_gzip = temp / "restored-gzip"
            run_cli(
                auth_file,
                "restore",
                str(plain_gzip_dir),
                str(restored_gzip),
                "--force",
                "--verify",
            )
            gzip_report = json.loads(run_cli(auth_file, "verify", str(restored_gzip), "--json"))
            assert "orders" in gzip_report["collections"], gzip_report["collections"]

            local_source = {"kind": "local", "path": str(plain_gzip_root)}
            local_browse = client.post(
                "/v1/import/browse", headers=headers, json={"source": local_source}
            )
            assert local_browse.status_code == 200, local_browse.text
            local_backups = local_browse.json()["result"]["backups"]
            assert local_backups, local_browse.text
            local_import = client.post(
                "/v1/import/run",
                headers=headers,
                json={
                    "source": local_source,
                    "location": local_backups[0]["location"],
                    "target_database": "imported-local",
                },
            )
            assert local_import.status_code == 200, local_import.text
            wait_import(client, admin, local_import.json()["result"]["id"])
            imported = client.post(
                "/imported-local/v1/exec",
                headers=headers,
                json={"op": "list_collections"},
            )
            assert imported.status_code == 200, imported.text
            assert "orders" in imported.json()["result"]["collections"], imported.text

            encrypted_source = {"kind": "local", "path": str(local_root)}
            encrypted_browse = client.post(
                "/v1/import/browse", headers=headers, json={"source": encrypted_source}
            )
            assert encrypted_browse.status_code == 200, encrypted_browse.text
            encrypted_backups = encrypted_browse.json()["result"]["backups"]
            encrypted_backup = next(
                item for item in encrypted_backups if item["manifest"]["source"] == "default"
            )
            encrypted_import = client.post(
                "/v1/import/run",
                headers=headers,
                json={
                    "source": encrypted_source,
                    "location": encrypted_backup["location"],
                    "target_database": "imported-encrypted",
                },
            )
            assert encrypted_import.status_code == 200, encrypted_import.text
            wait_import(client, admin, encrypted_import.json()["result"]["id"])
            imported = client.post(
                "/imported-encrypted/v1/exec",
                headers=headers,
                json={"op": "list_collections"},
            )
            assert imported.status_code == 200, imported.text
            assert "orders" in imported.json()["result"]["collections"], imported.text
            assert "secret" not in imported.json()["result"]["collections"], imported.text

            assert (data / "backup-keys").is_dir()
            assert (data / "backup-keys").stat().st_mode & 0o777 == 0o700
            view = client.get("/v1/backups", headers=headers).json()["result"]
            assert view["key_dir"].endswith("/backup-keys"), view
            assert view["default_identity_file"].endswith("/backup-identity.txt"), view
            assert view["default_local_path"].endswith("/backups"), view
            assert view["default_restic_repository"].endswith("/restic-repo"), view
            key_identity = temp / "nested" / "keys" / "api-identity.txt"
            key_recipient = temp / "nested" / "keys" / "api-recipient.txt"
            key_response = client.post(
                "/v1/backups/keygen",
                headers=headers,
                json={
                    "identity_file": str(key_identity),
                    "recipient_file": str(key_recipient),
                },
            )
            assert key_response.status_code == 200, key_response.text
            key = key_response.json()["result"]
            assert key["recipient"].startswith("age1"), key
            assert key_identity.is_file() and key_recipient.is_file()
            assert (key_identity.parent.stat().st_mode & 0o777) == 0o700
            assert (key_identity.stat().st_mode & 0o777) == 0o600
            assert key_recipient.read_text().strip() == key["recipient"]
            duplicate = client.post(
                "/v1/backups/keygen",
                headers=headers,
                json={
                    "identity_file": str(key_identity),
                    "recipient_file": str(key_recipient),
                },
            )
            assert duplicate.status_code == 400, duplicate.text

            restic_secret_path = temp / "generated-restic-password"
            secret_response = client.post(
                "/v1/backups/secret",
                headers=headers,
                json={"path": str(restic_secret_path)},
            )
            assert secret_response.status_code == 200, secret_response.text
            assert restic_secret_path.is_file()
            assert (restic_secret_path.stat().st_mode & 0o777) == 0o600
            assert len(restic_secret_path.read_text().strip()) == 64

            s3_job = {
                "id": "s3-plain",
                "database": "default",
                "enabled": True,
                "every_minutes": 1440,
                "destination": {
                    "kind": "s3",
                    "endpoint": f"http://127.0.0.1:{s3_port}",
                    "bucket": "backup-bucket",
                    "region": "auto",
                    "prefix": "prod/nonodb",
                    "access_key_env": "AWS_ACCESS_KEY_ID",
                    "secret_key_env": "AWS_SECRET_ACCESS_KEY",
                },
                "keep_last": 2,
                "verify": True,
            }
            assert (
                client.post("/v1/backups/jobs", headers=headers, json=s3_job).status_code
                == 200
            )
            assert client.post(
                "/v1/backups/jobs/s3-plain/run", headers=headers
            ).status_code == 200
            wait_backup(client, admin, "s3-plain")

            assert len(FakeS3.received) == 3, len(FakeS3.received)
            paths = [item["path"] for item in FakeS3.received]
            assert any(
                path.startswith("/backup-bucket/prod/nonodb/s3-plain/")
                and path.endswith("/data.mdb")
                for path in paths
            ), paths
            assert any(path.endswith("/manifest.json") for path in paths), paths
            assert all(
                item["authorization"].startswith("AWS4-HMAC-SHA256 ")
                for item in FakeS3.received
            )
            assert any(
                path.endswith("/backup-info.txt") for path in paths
            ), paths

            s3_source = {
                "kind": "s3",
                "endpoint": f"http://127.0.0.1:{s3_port}",
                "bucket": "backup-bucket",
                "region": "auto",
                "prefix": "prod/nonodb",
                "access_key_env": "AWS_ACCESS_KEY_ID",
                "secret_key_env": "AWS_SECRET_ACCESS_KEY",
            }
            s3_browse = client.post(
                "/v1/import/browse", headers=headers, json={"source": s3_source}
            )
            assert s3_browse.status_code == 200, s3_browse.text
            s3_backups = s3_browse.json()["result"]["backups"]
            assert s3_backups, s3_browse.text
            s3_import = client.post(
                "/v1/import/run",
                headers=headers,
                json={
                    "source": s3_source,
                    "location": s3_backups[0]["location"],
                    "target_database": "imported-s3",
                },
            )
            assert s3_import.status_code == 200, s3_import.text
            wait_import(client, admin, s3_import.json()["result"]["id"])
            imported = client.post(
                "/imported-s3/v1/exec",
                headers=headers,
                json={"op": "list_collections"},
            )
            assert imported.status_code == 200, imported.text
            assert "orders" in imported.json()["result"]["collections"], imported.text

            restic_job = {
                "id": "restic-job",
                "database": "default",
                "databases": ["default", "app1"],
                "enabled": True,
                "every_minutes": 1440,
                "destination": {
                    "kind": "restic",
                    "repository": str(temp / "restic-repo"),
                    "password_file": str(restic_password),
                    "binary": str(restic_binary),
                },
                "keep_last": 2,
                "verify": True,
            }
            assert (
                client.post("/v1/backups/jobs", headers=headers, json=restic_job).status_code
                == 200
            )
            assert client.post(
                "/v1/backups/jobs/restic-job/run", headers=headers
            ).status_code == 200
            wait_backup(client, admin, "restic-job")
            restic_commands = restic_log.read_text()
            assert "init" in restic_commands, restic_commands
            assert "backup" in restic_commands, restic_commands
            assert "forget --tag nonodb-job-restic-job-default --keep-last 2 --prune" in restic_commands
            assert "forget --tag nonodb-job-restic-job-app1 --keep-last 2 --prune" in restic_commands
            assert "check" in restic_commands, restic_commands
            assert str(restic_password) in restic_commands

            print("PASS  backups + import: compression, age, exclusions, keygen, local/S3 restore, restic")
            return 0
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        if log_file is not None:
            log_file.close()
        if server is not None:
            server.shutdown()
        if sys_exc := __import__("sys").exc_info()[0]:
            print(f"kept temp directory for inspection: {temp}")
        else:
            shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
