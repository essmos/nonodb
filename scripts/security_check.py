#!/usr/bin/env python3
"""Local security regression checks for nonodb.

Starts an isolated daemon on loopback TCP with a temporary auth registry. The
checks cover unauthenticated access, metrics protection, database-qualified
scopes, CORS behavior, malformed input handling, security headers, and auth
lockout.

Known hardening gaps are reported as warnings. Use --strict to fail on them as
well. No production data, token, or auth file is used.

Usage:
    cargo build -p nonodb-daemon -p nonodb-cli
    python3 scripts/security_check.py
    python3 scripts/security_check.py --strict
"""

from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
DAEMON = Path(os.environ.get("NONODB_DAEMON", ROOT / "target/debug/nonodb-daemon"))
CLI = Path(os.environ.get("NONODB_CLI", ROOT / "target/debug/nonodb-cli"))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def run_cli(auth_file: Path, *args: str) -> str:
    env = os.environ.copy()
    env["NONODB_AUTH_FILE"] = str(auth_file)
    proc = subprocess.run(
        [str(CLI), "auth", *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"nonodb-cli auth {' '.join(args)} failed: {proc.stderr.strip()}"
        )
    return proc.stdout.strip()


def wait_for_health(client: httpx.Client, proc: subprocess.Popen, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"daemon exited with code {proc.returncode}")
        try:
            if client.get("/healthz").status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise RuntimeError("daemon did not become healthy")


class Checks:
    def __init__(self) -> None:
        self.passed = 0
        self.warnings: list[str] = []
        self.failures: list[str] = []

    def check(self, name: str, ok: bool, detail: str = "", *, warn: bool = False) -> None:
        if ok:
            self.passed += 1
            print(f"PASS  {name}")
            return
        message = f"{name}: {detail}" if detail else name
        if warn:
            self.warnings.append(message)
            print(f"WARN  {message}")
        else:
            self.failures.append(message)
            print(f"FAIL  {message}")

    def is_ok(self) -> bool:
        return not self.failures


def post(
    client: httpx.Client,
    path: str,
    body: dict | None = None,
    *,
    token: str | None = None,
    content: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    request_headers = dict(headers or {})
    if token:
        request_headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        request_headers["Content-Type"] = "application/json"
    return client.post(path, json=body, content=content, headers=request_headers)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="also fail on known hardening warnings",
    )
    args = parser.parse_args()

    if not DAEMON.is_file():
        raise SystemExit(f"missing daemon binary: {DAEMON}; run cargo build")
    if not CLI.is_file():
        raise SystemExit(f"missing CLI binary: {CLI}; run cargo build")

    checks = Checks()
    temp = Path(tempfile.mkdtemp(prefix="nonodb-security-"))
    data = temp / "data"
    auth_file = temp / "auth.json"
    log_path = temp / "daemon.log"
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    proc: subprocess.Popen | None = None
    log_file = None

    try:
        data.mkdir()
        run_cli(auth_file, "init")
        admin_token = run_cli(auth_file, "token", "create", "admin")

        env = os.environ.copy()
        env.update(
            NONODB_DATA=str(data),
            NONODB_SOCK=str(temp / "nonodb.sock"),
            NONODB_TCP=f"127.0.0.1:{port}",
            NONODB_AUTH_FILE=str(auth_file),
            NONODB_LOG_FORMAT="json",
            NONODB_MAINTENANCE_INTERVAL_MS="100",
        )
        log_file = open(log_path, "w", encoding="utf-8")
        proc = subprocess.Popen(
            [str(DAEMON)],
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

        with httpx.Client(base_url=base_url, timeout=5.0, trust_env=False) as client:
            wait_for_health(client, proc, timeout=15.0)

            health = client.get("/healthz")
            checks.check("/healthz is public", health.status_code == 200, f"got {health.status_code}")

            metrics_anon = client.get("/metrics")
            checks.check(
                "/metrics rejects anonymous access",
                metrics_anon.status_code == 401,
                f"got {metrics_anon.status_code}",
            )

            exec_anon = post(client, "/v1/exec", {"op": "list_databases"})
            checks.check(
                "/v1/exec rejects anonymous access",
                exec_anon.status_code == 401,
                f"got {exec_anon.status_code}",
            )

            watch_anon = post(client, "/v1/watch", {"op": "watch", "coll": "_events"})
            checks.check(
                "/v1/watch rejects anonymous access",
                watch_anon.status_code == 401,
                f"got {watch_anon.status_code}",
            )

            invalid = post(
                client,
                "/v1/exec",
                {"op": "list_databases"},
                token="nodb_invalid_invalid",
            )
            checks.check(
                "invalid bearer token is rejected",
                invalid.status_code == 401,
                f"got {invalid.status_code}",
            )

            metrics_admin = client.get(
                "/metrics",
                headers={"Authorization": f"Bearer {admin_token}"},
            )
            checks.check(
                "admin can read /metrics",
                metrics_admin.status_code == 200,
                f"got {metrics_admin.status_code}",
            )

            create_db = post(
                client,
                "/v1/exec",
                {"op": "create_database", "name": "app1"},
                token=admin_token,
            )
            checks.check(
                "admin can create an isolated database",
                create_db.status_code == 200 and create_db.json().get("ok") is True,
                create_db.text[:200],
            )

            insert_app1 = post(
                client,
                "/app1/v1/exec",
                {"op": "insert", "coll": "orders", "doc": {"_id": "o1", "total": 10}},
                token=admin_token,
            )
            insert_default = post(
                client,
                "/v1/exec",
                {"op": "insert", "coll": "orders", "doc": {"_id": "d1", "total": 20}},
                token=admin_token,
            )
            checks.check(
                "admin can seed both databases",
                insert_app1.status_code == 200 and insert_default.status_code == 200,
                f"app1={insert_app1.status_code} default={insert_default.status_code}",
            )

            run_cli(
                auth_file,
                "principal",
                "add",
                "reviewer",
                "read:app1/orders,write:app1/orders",
            )
            reviewer_token = run_cli(auth_file, "token", "create", "reviewer")
            # The daemon watches the auth file and reloads valid changes in about
            # one second. A single failed probe here would count toward lockout.
            time.sleep(2.0)

            app1_read = post(
                client,
                "/app1/v1/exec",
                {"op": "find", "coll": "orders", "filter": {}},
                token=reviewer_token,
            )
            app1_rows = []
            if app1_read.status_code == 200:
                app1_rows = app1_read.json().get("result", [])
            checks.check(
                "scoped token can read its database/collection",
                app1_read.status_code == 200 and len(app1_rows) == 1,
                f"got {app1_read.status_code}: {app1_read.text[:200]}",
            )

            default_read = post(
                client,
                "/v1/exec",
                {"op": "find", "coll": "orders", "filter": {}},
                token=reviewer_token,
            )
            checks.check(
                "scoped token cannot read the same collection in default",
                default_read.status_code == 403,
                f"got {default_read.status_code}",
            )

            other_collection = post(
                client,
                "/app1/v1/exec",
                {"op": "find", "coll": "users", "filter": {}},
                token=reviewer_token,
            )
            checks.check(
                "scoped token cannot read another collection",
                other_collection.status_code == 403,
                f"got {other_collection.status_code}",
            )

            reviewer_metrics = client.get(
                "/metrics",
                headers={"Authorization": f"Bearer {reviewer_token}"},
            )
            checks.check(
                "scoped token cannot read /metrics",
                reviewer_metrics.status_code == 403,
                f"got {reviewer_metrics.status_code}",
            )

            reviewer_create_db = post(
                client,
                "/v1/exec",
                {"op": "create_database", "name": "evil"},
                token=reviewer_token,
            )
            checks.check(
                "scoped token cannot create databases",
                reviewer_create_db.status_code == 403,
                f"got {reviewer_create_db.status_code}",
            )

            preflight_headers = {
                "Origin": "https://evil.example",
                "Access-Control-Request-Method": "POST",
            }
            preflight = client.request("OPTIONS", "/v1/exec", headers=preflight_headers)
            checks.check(
                "CORS does not reflect an untrusted origin",
                "access-control-allow-origin" not in preflight.headers,
                preflight.headers.get("access-control-allow-origin", ""),
            )

            malformed = post(
                client,
                "/v1/exec",
                content=b"{",
                token=admin_token,
                headers={"Content-Type": "application/json"},
            )
            malformed_text = malformed.text
            checks.check(
                "malformed JSON is rejected without a server error",
                malformed.status_code == 400,
                f"got {malformed.status_code}",
            )
            checks.check(
                "malformed JSON does not expose a panic or backtrace",
                not any(
                    marker in malformed_text.lower()
                    for marker in ("traceback", "panic", "backtrace", "rust_backtrace")
                ),
                malformed_text[:200],
            )
            checks.check(
                "malformed JSON uses the normal JSON error envelope",
                malformed.headers.get("content-type", "").startswith("application/json"),
                f"content-type={malformed.headers.get('content-type', '')} body={malformed_text[:120]}",
                warn=True,
            )

            unknown = post(
                client,
                "/v1/exec",
                {"op": "__security_check_unknown__"},
                token=admin_token,
            )
            unknown_text = unknown.text
            checks.check(
                "unknown operation is a client error, not a server error",
                400 <= unknown.status_code < 500,
                f"got {unknown.status_code}",
            )
            checks.check(
                "unknown operation does not enumerate the full operation schema",
                not (
                    "expected one of" in unknown_text
                    or "create_database" in unknown_text
                    or "explain_sql" in unknown_text
                ),
                "error text exposes the operation enum",
                warn=True,
            )

            for label, response in (
                ("/healthz", health),
                ("/metrics (authenticated)", metrics_admin),
            ):
                checks.check(
                    f"nosniff header on {label}",
                    response.headers.get("x-content-type-options", "").lower() == "nosniff",
                    "missing X-Content-Type-Options: nosniff",
                    warn=True,
                )

            # Lockout is last because it blocks loopback for a short window.
            lockout_statuses = []
            for _ in range(8):
                response = post(
                    client,
                    "/v1/exec",
                    {"op": "list_databases"},
                    token="nodb_invalid_invalid",
                )
                lockout_statuses.append(response.status_code)
            checks.check(
                "repeated authentication failures trigger lockout",
                429 in lockout_statuses,
                f"statuses={lockout_statuses}",
            )
            blocked_admin = post(
                client,
                "/v1/exec",
                {"op": "list_databases"},
                token=admin_token,
            )
            checks.check(
                "lockout also blocks a valid token from the same peer",
                blocked_admin.status_code == 429,
                f"got {blocked_admin.status_code}",
            )

    except Exception as exc:
        checks.failures.append(f"security check crashed: {exc}")
        print(f"FAIL  security check crashed: {exc}")
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        if log_file is not None:
            log_file.close()

        if checks.failures:
            try:
                print("\n--- daemon log (tail) ---")
                lines = log_path.read_text(encoding="utf-8").splitlines()
                print("\n".join(lines[-20:]))
            except OSError:
                pass

        shutil.rmtree(temp, ignore_errors=True)

    print(
        f"\nsecurity check: {checks.passed} passed, "
        f"{len(checks.warnings)} warnings, {len(checks.failures)} failed"
    )
    if checks.warnings:
        print("hardening warnings:")
        for warning in checks.warnings:
            print(f"  - {warning}")
    if checks.failures:
        print("failures:")
        for failure in checks.failures:
            print(f"  - {failure}")
        return 1
    if args.strict and checks.warnings:
        print("strict mode: warnings are treated as failures")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
