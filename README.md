# health-sync

Personal health data sync service for moving selected measurements between
Zepp Life, Garmin Connect, Yazio, and Strava.

The repository is public. Do not commit credentials, tokens, local SQLite
databases, personal exports, logs containing health data, or VPS-only config.

See [docs/SYNC_PLAN.md](docs/SYNC_PLAN.md) for the current implementation plan.

## Local Development

```bash
uv run pytest
uv run ruff check .
```

## Commands

All commands default to `HEALTH_SYNC_DRY_RUN=true` unless the environment or CLI
overrides it.

```bash
health-sync init-db
health-sync zepp-garmin --dry-run
health-sync zepp-strava --dry-run
health-sync yazio-garmin --date 2026-06-21 --dry-run
health-sync run-once --dry-run
health-sync cleanup
health-sync serve
```

Use `--no-dry-run` only after reviewing dry-run logs and SQLite events.

## Production Schedule

The VPS service runs `health-sync serve` with Prefect schedules:

- Zepp to Garmin body composition: daily at 10:00 Europe/Berlin.
- Zepp to Strava profile weight: daily at 10:00 Europe/Berlin.
- Yazio to Garmin daily nutrition: every 3 hours via `0 */3 * * *`.
- SQLite cleanup: daily at 03:17 Europe/Berlin.

Scheduled production writes require `HEALTH_SYNC_DRY_RUN=false` in the
server-local `.env`.
