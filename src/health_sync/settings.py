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

    serve_interval_minutes: int

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
            serve_interval_minutes=_int_env("HEALTH_SYNC_SERVE_INTERVAL_MINUTES", 180),
        )
