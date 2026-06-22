"""Prefect flows for health-sync."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from zoneinfo import ZoneInfo

import httpx
from prefect import flow, task
from prefect.client.schemas.schedules import CronSchedule

from health_sync.adapters.garmin_mcp import GarminMcpClient
from health_sync.adapters.strava import StravaClient
from health_sync.adapters.yazio_mcp import YazioMcpClient
from health_sync.adapters.zepp_mcp import ZeppMcpClient
from health_sync.mappings import rolling_date_range
from health_sync.settings import Settings
from health_sync.state import CleanupResult, SyncState
from health_sync.sync import (
    SyncSummary,
    sync_yazio_to_garmin_once,
    sync_zepp_to_garmin_once,
    sync_zepp_weight_to_strava_once,
)


def _effective_dry_run(settings: Settings, dry_run: bool | None) -> bool:
    return settings.dry_run if dry_run is None else dry_run


def _today(settings: Settings) -> str:
    return datetime.now(ZoneInfo(settings.timezone)).date().isoformat()


def _date_range(
    settings: Settings,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, str]:
    if start_date and end_date:
        return start_date, end_date
    if end_date:
        end = date.fromisoformat(end_date)
        return rolling_date_range(settings.lookback_days, settings.timezone, end)
    return rolling_date_range(settings.lookback_days, settings.timezone)


@task(retries=2, retry_delay_seconds=60)
async def _sync_zepp_to_garmin_task(
    start_date: str,
    end_date: str,
    dry_run: bool,
) -> SyncSummary:
    settings = Settings.from_env()
    state = SyncState(settings.db_path)
    run_id = state.begin_run("sync_zepp_to_garmin", dry_run)
    try:
        summary = await sync_zepp_to_garmin_once(
            zepp_client=ZeppMcpClient(settings.zepp_mcp_url),
            garmin_client=GarminMcpClient(settings.garmin_mcp_url),
            state=state,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
            run_id=run_id,
        )
        state.finish_run(run_id, "success" if summary.failed == 0 else "failed")
        return summary
    except Exception as exc:
        state.finish_run(run_id, "failed", str(exc))
        raise


@task(retries=2, retry_delay_seconds=60)
async def _sync_zepp_weight_to_strava_task(
    start_date: str,
    end_date: str,
    dry_run: bool,
) -> SyncSummary:
    settings = Settings.from_env()
    state = SyncState(settings.db_path)
    run_id = state.begin_run("sync_zepp_weight_to_strava", dry_run)
    try:
        summary = await sync_zepp_weight_to_strava_once(
            zepp_client=ZeppMcpClient(settings.zepp_mcp_url),
            strava_client=StravaClient(
                client_id=settings.strava_client_id,
                client_secret=settings.strava_client_secret,
                refresh_token=settings.strava_refresh_token,
                access_token=settings.strava_access_token,
                expires_at=settings.strava_token_expires_at,
                token_file=settings.strava_token_file,
            ),
            state=state,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
            threshold_kg=settings.strava_weight_threshold_kg,
            run_id=run_id,
        )
        state.finish_run(run_id, "success" if summary.failed == 0 else "failed")
        return summary
    except Exception as exc:
        state.finish_run(run_id, "failed", str(exc))
        raise


@task(retries=2, retry_delay_seconds=60)
async def _sync_yazio_to_garmin_task(target_date: str, dry_run: bool) -> SyncSummary:
    settings = Settings.from_env()
    state = SyncState(settings.db_path)
    run_id = state.begin_run("sync_yazio_to_garmin", dry_run)
    try:
        summary = await sync_yazio_to_garmin_once(
            yazio_client=YazioMcpClient(settings.yazio_mcp_url),
            garmin_client=GarminMcpClient(settings.garmin_mcp_url),
            state=state,
            target_date=target_date,
            meal_time=settings.yazio_garmin_meal_time,
            entry_name=settings.yazio_garmin_entry_name,
            dry_run=dry_run,
            run_id=run_id,
        )
        state.finish_run(run_id, "success" if summary.failed == 0 else "failed")
        return summary
    except Exception as exc:
        state.finish_run(run_id, "failed", str(exc))
        raise


@task
def _cleanup_sqlite_task(vacuum: bool = False, analyze: bool = False) -> CleanupResult:
    settings = Settings.from_env()
    state = SyncState(settings.db_path)
    return state.cleanup(vacuum=vacuum, analyze=analyze)


@flow(name="sync-zepp-to-garmin")
async def sync_zepp_to_garmin_flow(
    start_date: str | None = None,
    end_date: str | None = None,
    dry_run: bool | None = None,
) -> SyncSummary:
    settings = Settings.from_env()
    start, end = _date_range(settings, start_date, end_date)
    return await _sync_zepp_to_garmin_task(start, end, _effective_dry_run(settings, dry_run))


@flow(name="sync-zepp-weight-to-strava")
async def sync_zepp_weight_to_strava_flow(
    start_date: str | None = None,
    end_date: str | None = None,
    dry_run: bool | None = None,
) -> SyncSummary:
    settings = Settings.from_env()
    start, end = _date_range(settings, start_date, end_date)
    return await _sync_zepp_weight_to_strava_task(start, end, _effective_dry_run(settings, dry_run))


@flow(name="sync-yazio-to-garmin")
async def sync_yazio_to_garmin_flow(
    target_date: str | None = None,
    dry_run: bool | None = None,
) -> SyncSummary:
    settings = Settings.from_env()
    return await _sync_yazio_to_garmin_task(
        target_date or _today(settings),
        _effective_dry_run(settings, dry_run),
    )


@flow(name="cleanup-sqlite")
def cleanup_sqlite_flow(vacuum: bool = False, analyze: bool = False) -> CleanupResult:
    return _cleanup_sqlite_task(vacuum=vacuum, analyze=analyze)


@flow(name="health-sync-run-once")
async def run_once_flow(dry_run: bool | None = None) -> list[SyncSummary]:
    zepp_garmin = await sync_zepp_to_garmin_flow(dry_run=dry_run)
    zepp_strava = await sync_zepp_weight_to_strava_flow(dry_run=dry_run)
    yazio_garmin = await sync_yazio_to_garmin_flow(dry_run=dry_run)
    return [zepp_garmin, zepp_strava, yazio_garmin]


def scheduled_deployments(settings: Settings | None = None) -> tuple:
    settings = settings or Settings.from_env()
    zepp_schedule = CronSchedule(cron=settings.zepp_cron, timezone=settings.timezone)
    yazio_schedule = CronSchedule(cron=settings.yazio_cron, timezone=settings.timezone)
    cleanup_schedule = CronSchedule(cron="17 3 * * *", timezone=settings.timezone)
    return (
        sync_zepp_to_garmin_flow.to_deployment(
            name="sync-zepp-to-garmin",
            schedule=zepp_schedule,
        ),
        sync_zepp_weight_to_strava_flow.to_deployment(
            name="sync-zepp-weight-to-strava",
            schedule=zepp_schedule,
        ),
        sync_yazio_to_garmin_flow.to_deployment(
            name="sync-yazio-to-garmin",
            schedule=yazio_schedule,
        ),
        cleanup_sqlite_flow.to_deployment(
            name="cleanup-sqlite",
            schedule=cleanup_schedule,
        ),
    )


def _local_prefect_api_url(settings: Settings) -> str:
    return f"http://{settings.prefect_server_host}:{settings.prefect_server_port}/api"


def _wait_for_prefect_api(
    api_url: str,
    process: subprocess.Popen[bytes] | None,
    timeout_seconds: int,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    health_url = f"{api_url.rstrip('/')}/health"
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"Prefect server exited early with code {process.returncode}.")
        try:
            response = httpx.get(health_url, timeout=2)
            if response.status_code == 200:
                return
        except Exception as exc:
            last_error = exc
        time.sleep(1)
    if last_error:
        raise TimeoutError(f"Prefect API did not become healthy at {health_url}: {last_error}")
    raise TimeoutError(f"Prefect API did not become healthy at {health_url}.")


def _stop_prefect_server(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


@contextmanager
def _prefect_api_for_serving(settings: Settings) -> Iterator[str]:
    if settings.prefect_api_url:
        yield settings.prefect_api_url
        return

    api_url = _local_prefect_api_url(settings)
    try:
        _wait_for_prefect_api(api_url, process=None, timeout_seconds=2)
        process = None
    except TimeoutError:
        env = os.environ.copy()
        env["PREFECT_API_URL"] = api_url
        env["PREFECT_SERVER_API_HOST"] = settings.prefect_server_host
        env["PREFECT_SERVER_API_PORT"] = str(settings.prefect_server_port)
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "prefect",
                "server",
                "start",
                "--host",
                settings.prefect_server_host,
                "--port",
                str(settings.prefect_server_port),
                "--no-ui",
                "--scheduler",
                "--late-runs",
                "--analytics-off",
            ],
            env=env,
        )
        _wait_for_prefect_api(
            api_url,
            process=process,
            timeout_seconds=settings.prefect_server_startup_timeout_seconds,
        )

    previous_api_url = os.environ.get("PREFECT_API_URL")
    os.environ["PREFECT_API_URL"] = api_url
    try:
        yield api_url
    finally:
        if previous_api_url is None:
            os.environ.pop("PREFECT_API_URL", None)
        else:
            os.environ["PREFECT_API_URL"] = previous_api_url
        if process is not None:
            _stop_prefect_server(process)


def serve_deployments() -> None:
    settings = Settings.from_env()
    from prefect import serve

    with _prefect_api_for_serving(settings):
        serve(*scheduled_deployments(settings))
