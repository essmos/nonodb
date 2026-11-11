"""Run a NonoDB background job worker.

Example:
    PYTHONPATH=client:workers/python:workers/python/examples \
      python3 -m nonodb_worker --module jobs_handlers --create-indexes
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import socket
import sys

from nonodb import NonoDB

from .jobs import DEFAULT_LEASE_SECONDS, JobQueue, JobRunner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NonoDB background job worker")
    parser.add_argument("--module", required=True, help="Python module with job handlers")
    parser.add_argument("--collection", default="_jobs", help="job collection name")
    parser.add_argument("--socket", default=os.environ.get("NONODB_SOCK"))
    parser.add_argument("--base-url", default=os.environ.get("NONODB_URL"))
    parser.add_argument("--token", default=os.environ.get("NONODB_TOKEN"))
    parser.add_argument("--worker-id", default=f"{socket.gethostname()}:{os.getpid()}")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    parser.add_argument("--create-indexes", action="store_true")
    parser.add_argument("--recover-expired", action="store_true")
    parser.add_argument("--once", action="store_true", help="run at most one job and exit")
    parser.add_argument("--log-level", default="INFO")
    return parser


def load_handlers(runner: JobRunner, module_name: str) -> None:
    module = importlib.import_module(module_name)
    register = getattr(module, "register", None)
    if callable(register):
        register(runner)
        return
    handlers = getattr(module, "HANDLERS", None)
    if not isinstance(handlers, dict):
        raise SystemExit(
            f"{module_name} must define register(runner) or a HANDLERS dict"
        )
    for job_type, handler in handlers.items():
        runner.register(str(job_type), handler)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    db = NonoDB(
        socket_path=args.socket,
        base_url=args.base_url,
        token=args.token,
    )
    try:
        queue = JobQueue(db.collection(args.collection))
        if args.create_indexes:
            created = queue.ensure_indexes()
            logging.info("ensured job indexes: %s", created)
        if args.recover_expired:
            recovered = queue.requeue_expired()
            logging.info("recovered %d expired job(s)", recovered)
        runner = JobRunner(queue, worker_id=args.worker_id)
        load_handlers(runner, args.module)
        if args.once:
            runner.run_once(lease_seconds=args.lease_seconds)
        else:
            runner.run_forever(
                poll_interval=args.poll_interval,
                lease_seconds=args.lease_seconds,
            )
    except KeyboardInterrupt:
        return 0
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
