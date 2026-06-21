"""Sync orchestration independent from Prefect runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from health_sync import mappings
from health_sync.state import SyncState


@dataclass(frozen=True)
class SyncSummary:
    flow_name: str
    attempted: int = 0
    applied: int = 0
    skipped: int = 0
    failed: int = 0
    dry_run: bool = False


async def sync_zepp_to_garmin_once(
    *,
    zepp_client: Any,
    garmin_client: Any,
    state: SyncState,
    start_date: str,
    end_date: str,
    dry_run: bool,
    run_id: int | None = None,
) -> SyncSummary:
    flow_name = "sync_zepp_to_garmin"
    await zepp_client.sync_body_measurements(start_date, end_date)
    measurements = await zepp_client.query_body_measurements(start_date, end_date)

    attempted = applied = skipped = failed = 0
    for measurement in measurements:
        attempted += 1
        try:
            payload = mappings.zepp_body_to_garmin(measurement)
            logical_key = mappings.zepp_body_logical_key(measurement)
            hash_value = mappings.payload_hash(payload)
            if state.already_applied(logical_key, "garmin", hash_value):
                skipped += 1
                state.record_event(
                    run_id=run_id,
                    flow_name=flow_name,
                    source="zepp",
                    target="garmin",
                    logical_key=logical_key,
                    source_timestamp=str(measurement.get("timestamp")),
                    payload_hash_value=hash_value,
                    status="skipped",
                    dry_run=dry_run,
                    payload=payload,
                    message="Payload hash already applied",
                )
                continue
            if dry_run:
                skipped += 1
                state.record_event(
                    run_id=run_id,
                    flow_name=flow_name,
                    source="zepp",
                    target="garmin",
                    logical_key=logical_key,
                    source_timestamp=str(measurement.get("timestamp")),
                    payload_hash_value=hash_value,
                    status="dry_run",
                    dry_run=True,
                    payload=payload,
                    message="Dry run; Garmin write skipped",
                )
                continue

            await garmin_client.add_body_composition(payload)
            applied += 1
            state.record_event(
                run_id=run_id,
                flow_name=flow_name,
                source="zepp",
                target="garmin",
                logical_key=logical_key,
                source_timestamp=str(measurement.get("timestamp")),
                payload_hash_value=hash_value,
                status="success",
                dry_run=False,
                payload=payload,
                update_state=True,
            )
        except Exception as exc:
            failed += 1
            state.record_event(
                run_id=run_id,
                flow_name=flow_name,
                source="zepp",
                target="garmin",
                logical_key=f"zepp:body:error:{measurement.get('timestamp', attempted)}",
                payload_hash_value=mappings.payload_hash(measurement),
                status="failed",
                dry_run=dry_run,
                payload=measurement,
                error=str(exc),
            )
    return SyncSummary(flow_name, attempted, applied, skipped, failed, dry_run)


async def sync_zepp_weight_to_strava_once(
    *,
    zepp_client: Any,
    strava_client: Any,
    state: SyncState,
    start_date: str,
    end_date: str,
    dry_run: bool,
    threshold_kg: float,
    run_id: int | None = None,
) -> SyncSummary:
    flow_name = "sync_zepp_weight_to_strava"
    measurements = await zepp_client.query_body_measurements(start_date, end_date, latest_only=True)
    latest = mappings.latest_body_measurement(measurements)
    if latest is None:
        return SyncSummary(flow_name, attempted=0, dry_run=dry_run)

    weight = float(latest["weight_kg"])
    payload = mappings.strava_weight_payload(weight)
    logical_key = mappings.strava_weight_logical_key()
    hash_value = mappings.payload_hash(payload)
    current_hash = state.current_hash(logical_key, "strava")

    if current_hash == hash_value:
        state.record_event(
            run_id=run_id,
            flow_name=flow_name,
            source="zepp",
            target="strava",
            logical_key=logical_key,
            source_timestamp=str(latest.get("timestamp")),
            payload_hash_value=hash_value,
            status="skipped",
            dry_run=dry_run,
            payload=payload,
            message="Weight hash already applied",
        )
        return SyncSummary(flow_name, attempted=1, skipped=1, dry_run=dry_run)

    previous_payload = state.current_payload(logical_key, "strava") if current_hash else None

    if previous_payload is not None:
        previous_weight = float(previous_payload["weight_kg"])
        if abs(weight - previous_weight) < threshold_kg:
            state.record_event(
                run_id=run_id,
                flow_name=flow_name,
                source="zepp",
                target="strava",
                logical_key=logical_key,
                source_timestamp=str(latest.get("timestamp")),
                payload_hash_value=hash_value,
                status="skipped",
                dry_run=dry_run,
                payload=payload,
                message=f"Weight change below threshold {threshold_kg} kg",
            )
            return SyncSummary(flow_name, attempted=1, skipped=1, dry_run=dry_run)

    if dry_run:
        state.record_event(
            run_id=run_id,
            flow_name=flow_name,
            source="zepp",
            target="strava",
            logical_key=logical_key,
            source_timestamp=str(latest.get("timestamp")),
            payload_hash_value=hash_value,
            status="dry_run",
            dry_run=True,
            payload=payload,
            message="Dry run; Strava write skipped",
        )
        return SyncSummary(flow_name, attempted=1, skipped=1, dry_run=True)

    try:
        await strava_client.update_athlete_weight(weight)
        state.record_event(
            run_id=run_id,
            flow_name=flow_name,
            source="zepp",
            target="strava",
            logical_key=logical_key,
            source_timestamp=str(latest.get("timestamp")),
            payload_hash_value=hash_value,
            status="success",
            dry_run=False,
            payload=payload,
            update_state=True,
        )
        return SyncSummary(flow_name, attempted=1, applied=1, dry_run=False)
    except Exception as exc:
        state.record_event(
            run_id=run_id,
            flow_name=flow_name,
            source="zepp",
            target="strava",
            logical_key=logical_key,
            source_timestamp=str(latest.get("timestamp")),
            payload_hash_value=hash_value,
            status="failed",
            dry_run=dry_run,
            payload=payload,
            error=str(exc),
        )
        return SyncSummary(flow_name, attempted=1, failed=1, dry_run=dry_run)


async def sync_yazio_to_garmin_once(
    *,
    yazio_client: Any,
    garmin_client: Any,
    state: SyncState,
    target_date: str,
    meal_time: str,
    entry_name: str,
    dry_run: bool,
    run_id: int | None = None,
) -> SyncSummary:
    flow_name = "sync_yazio_to_garmin"
    summary = await yazio_client.get_daily_summary(target_date)
    totals = mappings.yazio_summary_to_daily_totals(summary)
    payload = {
        "meal_date": target_date,
        "meal_time": meal_time,
        "name": entry_name,
        **totals,
    }
    logical_key = mappings.yazio_daily_logical_key(target_date)
    hash_value = mappings.payload_hash(payload)

    if state.already_applied(logical_key, "garmin", hash_value):
        state.record_event(
            run_id=run_id,
            flow_name=flow_name,
            source="yazio",
            target="garmin",
            logical_key=logical_key,
            source_timestamp=target_date,
            payload_hash_value=hash_value,
            status="skipped",
            dry_run=dry_run,
            payload=payload,
            message="Payload hash already applied",
        )
        return SyncSummary(flow_name, attempted=1, skipped=1, dry_run=dry_run)

    if dry_run:
        state.record_event(
            run_id=run_id,
            flow_name=flow_name,
            source="yazio",
            target="garmin",
            logical_key=logical_key,
            source_timestamp=target_date,
            payload_hash_value=hash_value,
            status="dry_run",
            dry_run=True,
            payload=payload,
            message="Dry run; Garmin nutrition write skipped",
        )
        return SyncSummary(flow_name, attempted=1, skipped=1, dry_run=True)

    try:
        await garmin_client.log_food(**payload)
        state.record_event(
            run_id=run_id,
            flow_name=flow_name,
            source="yazio",
            target="garmin",
            logical_key=logical_key,
            source_timestamp=target_date,
            payload_hash_value=hash_value,
            status="success",
            dry_run=False,
            payload=payload,
            update_state=True,
        )
        return SyncSummary(flow_name, attempted=1, applied=1, dry_run=False)
    except Exception as exc:
        state.record_event(
            run_id=run_id,
            flow_name=flow_name,
            source="yazio",
            target="garmin",
            logical_key=logical_key,
            source_timestamp=target_date,
            payload_hash_value=hash_value,
            status="failed",
            dry_run=dry_run,
            payload=payload,
            error=str(exc),
        )
        return SyncSummary(flow_name, attempted=1, failed=1, dry_run=dry_run)
