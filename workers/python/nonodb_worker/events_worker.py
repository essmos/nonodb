"""Run a NonoDB outbox event consumer.

Example:
    PYTHONPATH=client:workers/python:workers/python/examples \
      python3 -m nonodb_worker.events_worker \
        --module events_handlers --consumer-id order-sync
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import socket
import sys

from nonodb import NonoDB

from .events import EventConsumer, EventRunner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NonoDB outbox event worker")
    parser.add_argument("--module", required=True, help="Python module with event handlers")
    parser.add_argument("--events", default="_events", help="event collection name")
    parser.add_argument(
        "--checkpoints",
        default="_event_checkpoints",
        help="per-consumer checkpoint collection",
    )
    parser.add_argument(
        "--consumer-id",
        default=f"{socket.gethostname()}:{os.getpid()}",
    )
    parser.add_argument("--socket", default=os.environ.get("NONODB_SOCK"))
    parser.add_argument("--base-url", default=os.environ.get("NONODB_URL"))
    parser.add_argument("--token", default=os.environ.get("NONODB_TOKEN"))
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--reset", action="store_true", help="reset the consumer checkpoint")
    parser.add_argument("--once", action="store_true", help="process at most one batch and exit")
    parser.add_argument("--log-level", default="INFO")
    return parser


def load_handlers(runner: EventRunner, module_name: str) -> None:
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
    for event_type, handler in handlers.items():
        runner.register(str(event_type), handler)


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
        consumer = EventConsumer(
            db.collection(args.events),
            db.collection(args.checkpoints),
            args.consumer_id,
        )
        if args.reset:
            consumer.reset()
            logging.info("reset checkpoint for %s", args.consumer_id)
        runner = EventRunner(consumer)
        load_handlers(runner, args.module)
        if args.once:
            runner.run_once(limit=args.limit)
        else:
            runner.run_forever(
                poll_interval=args.poll_interval,
                limit=args.limit,
            )
    except KeyboardInterrupt:
        return 0
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
