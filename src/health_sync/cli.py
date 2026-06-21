"""Command line entrypoint."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, is_dataclass
from typing import Any

from health_sync.flows import (
    cleanup_sqlite_flow,
    run_once_flow,
    serve_deployments,
    sync_yazio_to_garmin_flow,
    sync_zepp_to_garmin_flow,
    sync_zepp_weight_to_strava_flow,
)
from health_sync.settings import Settings
from health_sync.state import SyncState


def _print_result(result: Any) -> None:
    if is_dataclass(result):
        print(asdict(result))
    elif isinstance(result, list):
        print([asdict(item) if is_dataclass(item) else item for item in result])
    else:
        print(result)


def _add_dry_run_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override HEALTH_SYNC_DRY_RUN for this command.",
    )


async def _run_async(args: argparse.Namespace) -> None:
    if args.command == "zepp-garmin":
        result = await sync_zepp_to_garmin_flow(
            start_date=args.start_date,
            end_date=args.end_date,
            dry_run=args.dry_run,
        )
    elif args.command == "zepp-strava":
        result = await sync_zepp_weight_to_strava_flow(
            start_date=args.start_date,
            end_date=args.end_date,
            dry_run=args.dry_run,
        )
    elif args.command == "yazio-garmin":
        result = await sync_yazio_to_garmin_flow(
            target_date=args.date,
            dry_run=args.dry_run,
        )
    elif args.command == "run-once":
        result = await run_once_flow(dry_run=args.dry_run)
    else:
        raise ValueError(f"Unknown async command: {args.command}")
    _print_result(result)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="health-sync")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Initialize the SQLite state database.")

    zepp_garmin = subparsers.add_parser("zepp-garmin", help="Sync Zepp body data to Garmin.")
    zepp_garmin.add_argument("--start-date")
    zepp_garmin.add_argument("--end-date")
    _add_dry_run_arg(zepp_garmin)

    zepp_strava = subparsers.add_parser("zepp-strava", help="Sync latest Zepp weight to Strava.")
    zepp_strava.add_argument("--start-date")
    zepp_strava.add_argument("--end-date")
    _add_dry_run_arg(zepp_strava)

    yazio_garmin = subparsers.add_parser("yazio-garmin", help="Sync Yazio daily totals to Garmin.")
    yazio_garmin.add_argument("--date")
    _add_dry_run_arg(yazio_garmin)

    run_once = subparsers.add_parser("run-once", help="Run all sync flows once.")
    _add_dry_run_arg(run_once)

    cleanup = subparsers.add_parser("cleanup", help="Clean old SQLite audit rows.")
    cleanup.add_argument("--vacuum", action="store_true")
    cleanup.add_argument("--analyze", action="store_true")

    subparsers.add_parser("serve", help="Serve scheduled Prefect deployments.")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = Settings.from_env()

    if args.command == "init-db":
        SyncState(settings.db_path)
        print({"db_path": str(settings.db_path), "status": "initialized"})
        return
    if args.command == "cleanup":
        result = cleanup_sqlite_flow(vacuum=args.vacuum, analyze=args.analyze)
        _print_result(result)
        return
    if args.command == "serve":
        serve_deployments()
        return

    asyncio.run(_run_async(args))


if __name__ == "__main__":
    main()
