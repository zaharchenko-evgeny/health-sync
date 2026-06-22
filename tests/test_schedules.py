from dataclasses import replace

from prefect.client.schemas.schedules import CronSchedule
from prefect.settings import PREFECT_API_URL

from health_sync.flows import (
    _local_prefect_api_url,
    _prefect_api_for_serving,
    scheduled_deployments,
)
from health_sync.settings import Settings


def test_schedule_settings_default_to_daily_zepp_and_three_hour_yazio_cron(monkeypatch):
    monkeypatch.delenv("HEALTH_SYNC_ZEPP_CRON", raising=False)
    monkeypatch.delenv("HEALTH_SYNC_YAZIO_CRON", raising=False)
    monkeypatch.delenv("HEALTH_SYNC_YAZIO_INTERVAL_MINUTES", raising=False)
    monkeypatch.delenv("HEALTH_SYNC_SERVE_INTERVAL_MINUTES", raising=False)

    settings = Settings.from_env()

    assert settings.zepp_cron == "0 10 * * *"
    assert settings.yazio_cron == "0 */3 * * *"


def test_yazio_cron_falls_back_to_legacy_interval_minutes(monkeypatch):
    monkeypatch.delenv("HEALTH_SYNC_YAZIO_CRON", raising=False)
    monkeypatch.delenv("HEALTH_SYNC_YAZIO_INTERVAL_MINUTES", raising=False)
    monkeypatch.setenv("HEALTH_SYNC_YAZIO_INTERVAL_MINUTES", "180")

    settings = Settings.from_env()

    assert settings.yazio_cron == "0 */3 * * *"


def test_scheduled_deployments_use_separate_zepp_and_yazio_schedules(monkeypatch):
    monkeypatch.setenv("HEALTH_SYNC_ZEPP_CRON", "0 10 * * *")
    monkeypatch.setenv("HEALTH_SYNC_YAZIO_CRON", "0 */3 * * *")
    monkeypatch.setenv("HEALTH_SYNC_TIMEZONE", "Europe/Berlin")
    settings = Settings.from_env()

    deployments = scheduled_deployments(settings)
    schedules = {
        deployment.name: deployment.schedules[0].schedule for deployment in deployments
    }

    zepp_garmin_schedule = schedules["sync-zepp-to-garmin"]
    zepp_strava_schedule = schedules["sync-zepp-weight-to-strava"]
    yazio_schedule = schedules["sync-yazio-to-garmin"]

    assert isinstance(zepp_garmin_schedule, CronSchedule)
    assert zepp_garmin_schedule.cron == "0 10 * * *"
    assert zepp_garmin_schedule.timezone == "Europe/Berlin"
    assert isinstance(zepp_strava_schedule, CronSchedule)
    assert zepp_strava_schedule.cron == "0 10 * * *"
    assert zepp_strava_schedule.timezone == "Europe/Berlin"
    assert isinstance(yazio_schedule, CronSchedule)
    assert yazio_schedule.cron == "0 */3 * * *"
    assert yazio_schedule.timezone == "Europe/Berlin"


def test_local_prefect_api_defaults_to_loopback_server(monkeypatch):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    monkeypatch.delenv("HEALTH_SYNC_PREFECT_SERVER_HOST", raising=False)
    monkeypatch.delenv("HEALTH_SYNC_PREFECT_SERVER_PORT", raising=False)

    settings = Settings.from_env()

    assert settings.prefect_api_url is None
    assert _local_prefect_api_url(settings) == "http://127.0.0.1:4200/api"


def test_prefect_api_context_updates_prefect_settings(monkeypatch):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    settings = replace(
        Settings.from_env(),
        prefect_api_url="http://127.0.0.1:9999/api",
    )

    assert PREFECT_API_URL.value() is None
    with _prefect_api_for_serving(settings) as api_url:
        assert api_url == "http://127.0.0.1:9999/api"
        assert PREFECT_API_URL.value() == "http://127.0.0.1:9999/api"
    assert PREFECT_API_URL.value() is None
