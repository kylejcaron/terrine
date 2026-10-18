from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from terrine.errors import TerrineError
from terrine.runtime.control import ControlClient, ControlEndpoint
from terrine.runtime.recipe import DestinationRef


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="terrine")
    commands = parser.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan", help="validate and canonicalize a saved refresh plan")
    plan.add_argument("plan_file", type=Path)

    run = commands.add_parser("run", help="execute a saved refresh plan through its owner")
    run.add_argument("plan_file", type=Path)
    run.add_argument("--request-key")
    run.add_argument("--destination-database")
    run.add_argument("--destination-schema")
    run.add_argument("--destination-dataset")

    status = commands.add_parser("status", help="read an owner's durable run status")
    status.add_argument("run_id")

    events = commands.add_parser("events", help="read an owner's bounded run event page")
    events.add_argument("run_id")
    events.add_argument("--after")
    events.add_argument("--limit", type=int, default=100)

    cancel = commands.add_parser("cancel", help="request cancellation through the owner")
    cancel.add_argument("run_id")
    return parser


def _json(value: Any) -> str:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value, sort_keys=True, default=str)


def _client() -> ControlClient:
    endpoint = os.environ.get("TERRINE_RUNTIME_SOCKET")
    namespace = os.environ.get("TERRINE_RUNTIME_NAMESPACE")
    if not endpoint or not namespace:
        raise ValueError("TERRINE_RUNTIME_SOCKET and TERRINE_RUNTIME_NAMESPACE are required")
    return ControlClient(ControlEndpoint(Path(endpoint)), namespace=namespace)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        client = _client()
        if args.command == "plan":
            serialized = client.plan(args.plan_file.read_bytes())
            print(serialized)
        elif args.command == "run":
            destination_values = (
                args.destination_database,
                args.destination_schema,
                args.destination_dataset,
            )
            if args.request_key and not all(destination_values):
                raise ValueError("a request key requires all destination fields")
            if any(destination_values) and not all(destination_values):
                raise ValueError("all destination fields must be supplied together")
            destination = DestinationRef(*destination_values) if all(destination_values) else None
            receipt = client.run(
                args.plan_file.read_bytes(),
                request_key=args.request_key,
                destination=destination,
            )
            print(_json(receipt))
        elif args.command == "status":
            print(_json(client.get_run(args.run_id)))
        elif args.command == "events":
            page = client.read_run_events(args.run_id, after=args.after, limit=args.limit)
            print(_json(page))
        else:
            print(_json(client.cancel_run(args.run_id)))
    except (OSError, ValueError, TerrineError) as error:
        code = getattr(error, "code", "terrine.runtime.cli_error")
        print(json.dumps({"code": code}, sort_keys=True), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
