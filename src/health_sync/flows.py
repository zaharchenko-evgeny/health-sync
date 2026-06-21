"""Prefect flows for health-sync."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

from prefect import flow, task

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


def serve_deployments() -> None:
    settings = Settings.from_env()
    from datetime import timedelta

    from prefect import serve

    interval = timedelta(minutes=settings.serve_interval_minutes)
    serve(
        sync_zepp_to_garmin_flow.to_deployment(
            name="sync-zepp-to-garmin",
            interval=interval,
        ),
        sync_zepp_weight_to_strava_flow.to_deployment(
            name="sync-zepp-weight-to-strava",
            interval=interval,
        ),
        sync_yazio_to_garmin_flow.to_deployment(
            name="sync-yazio-to-garmin",
            interval=interval,
        ),
        cleanup_sqlite_flow.to_deployment(
            name="cleanup-sqlite",
            cron="17 3 * * *",
        ),
    )
