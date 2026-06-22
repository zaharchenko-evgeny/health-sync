"""Environment-backed settings for health-sync."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def _interval_minutes_to_cron(minutes: int) -> str:
    if minutes <= 0:
        raise ValueError("Interval minutes must be positive.")
    if minutes % 60 == 0:
        hours = minutes // 60
        if hours == 1:
            return "0 * * * *"
        if 24 % hours == 0:
            return f"0 */{hours} * * *"
    if minutes < 60 and 60 % minutes == 0:
        return f"*/{minutes} * * * *"
    raise ValueError(
        "Cannot convert HEALTH_SYNC_YAZIO_INTERVAL_MINUTES to a stable cron expression; "
        "set HEALTH_SYNC_YAZIO_CRON instead."
    )


def _cron_env(name: str, default: str, legacy_interval_name: str | None = None) -> str:
    value = os.getenv(name)
    if value:
        return value
    if legacy_interval_name:
        legacy_value = os.getenv(legacy_interval_name)
        if legacy_value:
            return _interval_minutes_to_cron(int(legacy_value))
    return default


@dataclass(frozen=True)
class Settings:
    db_path: Path
    dry_run: bool
    timezone: str
    lookback_days: int

    garmin_mcp_url: str
    yazio_mcp_url: str
    zepp_mcp_url: str

    yazio_garmin_meal_time: str
    yazio_garmin_entry_name: str

    strava_client_id: str | None
    strava_client_secret: str | None
    strava_refresh_token: str | None
    strava_access_token: str | None
    strava_token_expires_at: int
    strava_token_file: Path | None
    strava_weight_threshold_kg: float

    zepp_cron: str
    yazio_cron: str
    prefect_api_url: str | None
    prefect_server_host: str
    prefect_server_port: int
    prefect_server_startup_timeout_seconds: int

    @classmethod
    def from_env(cls) -> Settings:
        token_file = os.getenv("STRAVA_TOKEN_FILE")
        return cls(
            db_path=Path(os.getenv("HEALTH_SYNC_DB_PATH", "/data/health-sync.sqlite3")),
            dry_run=_bool_env("HEALTH_SYNC_DRY_RUN", True),
            timezone=os.getenv("HEALTH_SYNC_TIMEZONE", "Europe/Berlin"),
            lookback_days=_int_env("HEALTH_SYNC_LOOKBACK_DAYS", 3),
            garmin_mcp_url=os.getenv("GARMIN_MCP_URL", "http://127.0.0.1:8910/mcp"),
            yazio_mcp_url=os.getenv("YAZIO_MCP_URL", "http://127.0.0.1:8911/mcp"),
            zepp_mcp_url=os.getenv("ZEPP_MCP_URL", "http://127.0.0.1:8912/mcp"),
            yazio_garmin_meal_time=os.getenv("YAZIO_GARMIN_MEAL_TIME", "21:00:00"),
            yazio_garmin_entry_name=os.getenv("YAZIO_GARMIN_ENTRY_NAME", "Yazio daily total"),
            strava_client_id=os.getenv("STRAVA_CLIENT_ID") or None,
            strava_client_secret=os.getenv("STRAVA_CLIENT_SECRET") or None,
            strava_refresh_token=os.getenv("STRAVA_REFRESH_TOKEN") or None,
            strava_access_token=os.getenv("STRAVA_ACCESS_TOKEN") or None,
            strava_token_expires_at=_int_env("STRAVA_TOKEN_EXPIRES_AT", 0),
            strava_token_file=Path(token_file) if token_file else Path("/data/strava-token.json"),
            strava_weight_threshold_kg=_float_env("STRAVA_WEIGHT_THRESHOLD_KG", 0.1),
            zepp_cron=os.getenv("HEALTH_SYNC_ZEPP_CRON", "0 10 * * *"),
            yazio_cron=_cron_env(
                "HEALTH_SYNC_YAZIO_CRON",
                "0 */3 * * *",
                legacy_interval_name="HEALTH_SYNC_YAZIO_INTERVAL_MINUTES",
            ),
            prefect_api_url=os.getenv("PREFECT_API_URL") or None,
            prefect_server_host=os.getenv("HEALTH_SYNC_PREFECT_SERVER_HOST", "127.0.0.1"),
            prefect_server_port=_int_env("HEALTH_SYNC_PREFECT_SERVER_PORT", 4200),
            prefect_server_startup_timeout_seconds=_int_env(
                "HEALTH_SYNC_PREFECT_SERVER_STARTUP_TIMEOUT_SECONDS",
                60,
            ),
        )
