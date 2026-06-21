# Health Sync Plan

## Goal

Build a small, deterministic sync service that keeps selected health metrics aligned across existing personal services:

- Zepp Life to Garmin: daily weight and body composition.
- Zepp Life to Strava: latest athlete profile weight.
- Yazio to Garmin: daily calories and macros.

The sync project lives in this public Git repository:

```text
/Users/Evgenii.Zakharchenko/Personal/health-sync
```

The VPS deployment should auto-update when new commits are pushed to the repository, matching the existing MCP auto-update pattern already used on the Oracle VPS.

## Non-Goals

- Do not build an LLM-driven sync loop.
- Do not use Hermes as the sync engine.
- Do not commit credentials, API tokens, SQLite databases, logs, exports, or personal health data.
- Do not mirror every individual Yazio food item in the first version.
- Do not treat Strava as a historical weight ledger; Strava only receives the current profile weight.

## Architecture

Use a separate Python service named `health-sync`.

The service should use a well-known workflow library for scheduling, retries, backfills, run state, and logs. The preferred library is Prefect because it is Python-native and lightweight enough for the current VPS.

Runtime components:

- `health-sync`: Python package with Prefect flows and adapters.
- SQLite: local idempotency and audit ledger.
- Docker: containerized service on the VPS.
- Existing MCP backends: private loopback endpoints for Zepp, Garmin, and Yazio.
- Strava API: direct API client for athlete profile weight updates.

Private MCP endpoints on the VPS:

```text
Garmin: http://127.0.0.1:8910/mcp
Yazio:  http://127.0.0.1:8911/mcp
Zepp:   http://127.0.0.1:8912/mcp
```

The sync service talks to those private loopback endpoints from the VPS. It should not route through the public OAuth gateways.

## Repository Layout

Target structure:

```text
health-sync/
  README.md
  deploy/
    docker-compose.service.yml
    update-mcp-snippet.sh
  docs/
    SYNC_PLAN.md
  src/
    health_sync/
      __init__.py
      flows.py
      settings.py
      state.py
      mappings.py
      adapters/
        __init__.py
        garmin_mcp.py
        strava.py
        yazio_mcp.py
        zepp_mcp.py
  tests/
  Dockerfile
  pyproject.toml
  .env.example
```

## Public Repository Policy

This repository is public, so tracked files must be safe to publish.

Allowed:

- Source code.
- Tests and fake fixtures.
- Documentation.
- `.env.example` with variable names and placeholder values only.
- Dockerfile and deployment templates without secrets.

Forbidden:

- Real Strava client secrets, refresh tokens, or access tokens.
- Garmin token bundles.
- Zepp app tokens.
- Yazio credentials.
- SQLite state databases.
- Health exports or logs containing personal measurements.
- Server-local `.env` files.

The root `.gitignore` should ignore `.env`, SQLite files, virtualenvs, local caches, and IDE metadata.

## Required Garmin MCP Tool Surface

Garmin MCP should expose these tools for sync and manual inspection:

- `get_weigh_ins`
- `get_daily_weigh_ins`
- `add_weigh_in`
- `add_weigh_in_with_timestamps`
- `get_body_composition`
- `add_body_composition`

`get_body_composition` already exists in the Garmin health tool module. `add_body_composition` should wrap the existing `garminconnect.Garmin.add_body_composition(...)` library method.

## Flows

### `sync_zepp_to_garmin`

Purpose: copy Zepp body measurements into Garmin.

Steps:

1. Call Zepp `sync_data` for `body_measurements`.
2. Query recent Zepp body measurements, normally a rolling 3-day window.
3. Normalize each measurement.
4. Check SQLite idempotency state.
5. Write to Garmin with `add_body_composition`.
6. Record success, skip, or failure in SQLite.

Field mapping:

```text
Zepp weight_kg              -> Garmin weight
Zepp body_fat_pct           -> Garmin percent_fat
Zepp water_pct              -> Garmin percent_hydration
Zepp muscle_mass_kg         -> Garmin muscle_mass
Zepp bone_mass_kg           -> Garmin bone_mass
Zepp basal_metabolism_kcal  -> Garmin basal_met
Zepp metabolic_age          -> Garmin metabolic_age
Zepp visceral_fat_score     -> Garmin visceral_fat_rating
Zepp bmi                    -> Garmin bmi
```

### `sync_zepp_weight_to_strava`

Purpose: update Strava athlete profile weight from latest Zepp weight.

Steps:

1. Read the latest Zepp body measurement.
2. Compare with the latest synced Strava state.
3. If changed, update Strava athlete weight.
4. Store the applied weight hash in SQLite.

Notes:

- This sync represents current profile weight only.
- It should not attempt to backfill historical daily weights into Strava.

### `sync_yazio_to_garmin`

Purpose: copy Yazio nutrition totals into Garmin.

Initial version:

1. Call Yazio `get_user_daily_summary` for each target date.
2. Aggregate calories, carbs, protein, and fat.
3. Write one Garmin quick-add food entry per day:

```text
Name: Yazio daily total
Meal date: target date
Meal time: configurable default, e.g. 21:00:00
Calories/macros: Yazio daily totals
```

Later version:

- Optionally write one quick-add entry per Yazio meal.
- Do not implement per-food mirroring until the aggregate sync is stable.

## SQLite State And Cleanup Policy

SQLite is an audit and idempotency ledger, not the source of truth for health analytics.

Suggested tables:

- `sync_runs`: one row per flow run.
- `sync_events`: one row per attempted source-to-target write.
- `sync_state`: compact latest-known state per logical sync key.
- `dead_letters`: failed events requiring manual review.

Retention:

- Successful `sync_events`: keep 90 days.
- Failed `sync_events`: keep 180 days or until resolved.
- `sync_runs`: keep 90 days.
- Resolved `dead_letters`: keep 180 days.
- Unresolved `dead_letters`: keep until manually resolved.
- `sync_state`: keep indefinitely, compacted by logical key.

Do not delete compact idempotency records needed to avoid duplicates:

- Zepp to Garmin body composition key: source timestamp/date, payload hash, target.
- Yazio to Garmin daily nutrition key: date, aggregate hash, target.
- Zepp to Strava weight key: latest applied weight hash.

Cleanup flow:

```text
cleanup_sqlite
  daily:
    delete old terminal sync_events
    delete old sync_runs
    delete old resolved dead_letters
    keep sync_state
    wal_checkpoint(TRUNCATE)
  weekly:
    VACUUM
    ANALYZE
```

Safety rules:

- Cleanup only deletes terminal rows: `success`, `skipped`, `resolved`.
- Cleanup never deletes `pending`, `running`, `retrying`, or unresolved `failed` rows.
- Cleanup runs in a transaction.
- Cleanup logs deleted row counts by table.
- Alert or log warning if the SQLite file grows beyond 100 MB.

## Deployment Plan

Target VPS layout:

```text
/opt/mcp/
  docker-compose.yml
  update-mcp.sh
  health-sync/
    .git/
    .env              # server-only, never committed
    data/             # SQLite volume or bind mount, never committed
```

Add a `health-sync` service to the existing `/opt/mcp/docker-compose.yml`.

The public-safe compose snippet lives at:

```text
deploy/docker-compose.service.yml
```

Expected service properties:

- Build context: `/opt/mcp/health-sync`.
- Restart policy: `unless-stopped`.
- Memory limit: conservative, e.g. `512m`.
- Network: same private Docker network as MCP services if needed.
- Bind or volume mount for SQLite state.
- Environment loaded from `/opt/mcp/health-sync/.env`.

The service should run Prefect schedules or a lightweight entrypoint that starts the scheduled flows.

Set `PREFECT_HOME=/data/prefect` in the server-local `.env` so Prefect's local runtime state is stored in the mounted Docker volume instead of the container filesystem. The container also disables Prefect analytics/telemetry by default because this service uses a local temporary Prefect server rather than Prefect Cloud.

When running ad hoc smoke commands inside the already-running `health-sync` container, use an isolated Prefect home so the manual command does not contend with the scheduler's temporary server:

```bash
docker compose exec -T health-sync env PREFECT_HOME=/tmp/prefect-smoke health-sync zepp-garmin --dry-run
docker compose exec -T health-sync env PREFECT_HOME=/tmp/prefect-smoke health-sync cleanup
```

Run these smoke commands sequentially, not in parallel. The sync audit SQLite database is safe to share, but Prefect's temporary server state can lock if multiple ad hoc flow runners use the same `PREFECT_HOME`.

## Auto-Redeploy On Push

The existing VPS auto-update pattern should be extended for `health-sync`.

Current pattern:

1. Timer runs `/opt/mcp/update-mcp.sh`.
2. Script fetches each repo.
3. If upstream has a new commit, it fast-forwards.
4. Docker rebuilds only the changed service.
5. If the build succeeds, Docker restarts that service.
6. If the build fails, the previous running container stays in place.

Add a new updater entry:

```bash
update_one health-sync health-sync
```

The public-safe updater snippet lives at:

```text
deploy/update-mcp-snippet.sh
```

Expected behavior:

- Push to `github.com/zaharchenko-evgeny/health-sync`.
- VPS timer detects the new commit.
- `/opt/mcp/health-sync` fast-forwards.
- `docker compose build health-sync` runs.
- `docker compose up -d health-sync` restarts only the sync service.

This repository must contain only public-safe code and templates. Runtime secrets stay only in `/opt/mcp/health-sync/.env` on the VPS.

## Rollout Plan

1. Implement Garmin `add_body_composition` MCP tool and deploy it.
2. Build `health-sync` package skeleton with fake adapters and unit tests.
3. Add real MCP adapters.
4. Add SQLite state and cleanup flow.
5. Add dry-run mode.
6. Run dry-run for the last 7 days.
7. Enable Zepp to Garmin body composition writes.
8. Enable Zepp to Strava weight writes.
9. Enable Yazio to Garmin daily nutrition writes.
10. Review logs for one week before expanding backfill windows.

## Open Decisions

- Prefect local mode versus Prefect server UI on the VPS.
- Exact Yazio daily total meal time in Garmin.
- Whether Yazio to Garmin should be one daily quick-add or per-meal quick-add in v1.
- Whether Strava updates should happen on every weight change or only when the change exceeds a threshold, e.g. 0.1 kg.
- Whether cleanup should run as a Prefect flow only or also be callable from a CLI command.
