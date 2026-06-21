"""Pure mapping and hashing helpers."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def payload_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def rolling_date_range(
    lookback_days: int,
    timezone: str,
    end_date: date | None = None,
) -> tuple[str, str]:
    if end_date is None:
        end_date = datetime.now(ZoneInfo(timezone)).date()
    start_date = end_date - timedelta(days=max(lookback_days - 1, 0))
    return start_date.isoformat(), end_date.isoformat()


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def zepp_body_to_garmin(measurement: dict[str, Any]) -> dict[str, Any]:
    weight = _optional_float(measurement.get("weight_kg"))
    if weight is None or weight <= 0:
        raise ValueError("Zepp body measurement is missing a positive weight_kg")

    timestamp = measurement.get("timestamp")
    if not timestamp:
        raise ValueError("Zepp body measurement is missing timestamp")

    payload = {
        "timestamp": str(timestamp),
        "weight": weight,
        "percent_fat": _optional_float(measurement.get("body_fat_pct")),
        "percent_hydration": _optional_float(measurement.get("water_pct")),
        "muscle_mass": _optional_float(measurement.get("muscle_mass_kg")),
        "bone_mass": _optional_float(measurement.get("bone_mass_kg")),
        "basal_met": _optional_float(measurement.get("basal_metabolism_kcal")),
        "metabolic_age": _optional_float(measurement.get("metabolic_age")),
        "visceral_fat_rating": _optional_float(measurement.get("visceral_fat_score")),
        "bmi": _optional_float(measurement.get("bmi")),
    }
    return {key: value for key, value in payload.items() if value is not None}


def zepp_body_logical_key(measurement: dict[str, Any]) -> str:
    return f"zepp:body:{measurement['timestamp']}"


def latest_body_measurement(measurements: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not measurements:
        return None
    return max(measurements, key=lambda item: str(item.get("timestamp", "")))


def yazio_summary_to_daily_totals(summary: dict[str, Any]) -> dict[str, float]:
    meals = summary.get("meals") or {}
    totals = {"calories": 0.0, "carbs": 0.0, "protein": 0.0, "fat": 0.0}
    for meal in meals.values():
        nutrients = (meal or {}).get("nutrients") or {}
        totals["calories"] += float(nutrients.get("energy.energy") or 0)
        totals["carbs"] += float(nutrients.get("nutrient.carb") or 0)
        totals["protein"] += float(nutrients.get("nutrient.protein") or 0)
        totals["fat"] += float(nutrients.get("nutrient.fat") or 0)
    return {key: round(value, 2) for key, value in totals.items()}


def yazio_daily_logical_key(target_date: str) -> str:
    return f"yazio:nutrition:{target_date}"


def strava_weight_payload(weight_kg: float) -> dict[str, float]:
    return {"weight_kg": round(float(weight_kg), 2)}


def strava_weight_logical_key() -> str:
    return "strava:athlete:weight"
