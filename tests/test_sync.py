from health_sync import mappings
from health_sync.state import SyncState
from health_sync.sync import (
    sync_yazio_to_garmin_once,
    sync_zepp_to_garmin_once,
    sync_zepp_weight_to_strava_once,
)


class FakeZepp:
    def __init__(self, measurements):
        self.measurements = measurements
        self.sync_calls = []

    async def sync_body_measurements(self, start_date, end_date):
        self.sync_calls.append((start_date, end_date))

    async def query_body_measurements(self, start_date, end_date, latest_only=False):
        if latest_only and self.measurements:
            return [mappings.latest_body_measurement(self.measurements)]
        return self.measurements


class FakeGarmin:
    def __init__(self):
        self.body_payloads = []
        self.food_payloads = []

    async def add_body_composition(self, payload):
        self.body_payloads.append(payload)
        return {"ok": True}

    async def log_food(self, **payload):
        self.food_payloads.append(payload)
        return {"ok": True}


class FakeYazio:
    async def get_daily_summary(self, target_date):
        return {
            "meals": {
                "breakfast": {
                    "nutrients": {
                        "energy.energy": 500,
                        "nutrient.carb": 50,
                        "nutrient.fat": 15,
                        "nutrient.protein": 30,
                    }
                },
                "dinner": {
                    "nutrients": {
                        "energy.energy": 700,
                        "nutrient.carb": 60,
                        "nutrient.fat": 20,
                        "nutrient.protein": 40,
                    }
                },
            }
        }


class FakeStrava:
    def __init__(self):
        self.weights = []

    async def update_athlete_weight(self, weight_kg):
        self.weights.append(weight_kg)
        return {"weight": weight_kg}


def measurement(weight=86.2, timestamp="2026-06-21T07:30:00"):
    return {
        "timestamp": timestamp,
        "weight_kg": weight,
        "body_fat_pct": 20.1,
        "water_pct": 55.2,
    }


async def test_zepp_to_garmin_dry_run_skips_write(tmp_path):
    state = SyncState(tmp_path / "state.sqlite3")
    zepp = FakeZepp([measurement()])
    garmin = FakeGarmin()

    result = await sync_zepp_to_garmin_once(
        zepp_client=zepp,
        garmin_client=garmin,
        state=state,
        start_date="2026-06-19",
        end_date="2026-06-21",
        dry_run=True,
    )

    assert result.attempted == 1
    assert result.skipped == 1
    assert garmin.body_payloads == []
    assert state.current_hash("zepp:body:2026-06-21T07:30:00", "garmin") is None


async def test_zepp_to_garmin_writes_once_then_skips_duplicate(tmp_path):
    state = SyncState(tmp_path / "state.sqlite3")
    zepp = FakeZepp([measurement()])
    garmin = FakeGarmin()

    first = await sync_zepp_to_garmin_once(
        zepp_client=zepp,
        garmin_client=garmin,
        state=state,
        start_date="2026-06-19",
        end_date="2026-06-21",
        dry_run=False,
    )
    second = await sync_zepp_to_garmin_once(
        zepp_client=zepp,
        garmin_client=garmin,
        state=state,
        start_date="2026-06-19",
        end_date="2026-06-21",
        dry_run=False,
    )

    assert first.applied == 1
    assert second.skipped == 1
    assert len(garmin.body_payloads) == 1


async def test_yazio_to_garmin_logs_daily_total(tmp_path):
    state = SyncState(tmp_path / "state.sqlite3")
    garmin = FakeGarmin()

    result = await sync_yazio_to_garmin_once(
        yazio_client=FakeYazio(),
        garmin_client=garmin,
        state=state,
        target_date="2026-06-21",
        meal_time="21:00:00",
        entry_name="Yazio daily total",
        dry_run=False,
    )

    assert result.applied == 1
    assert garmin.food_payloads == [
        {
            "meal_date": "2026-06-21",
            "meal_time": "21:00:00",
            "name": "Yazio daily total",
            "calories": 1200.0,
            "carbs": 110.0,
            "protein": 70.0,
            "fat": 35.0,
        }
    ]


async def test_zepp_weight_to_strava_respects_threshold(tmp_path):
    state = SyncState(tmp_path / "state.sqlite3")
    state.record_event(
        run_id=None,
        flow_name="sync_zepp_weight_to_strava",
        source="zepp",
        target="strava",
        logical_key=mappings.strava_weight_logical_key(),
        payload_hash_value=mappings.payload_hash({"weight_kg": 86.2}),
        status="success",
        dry_run=False,
        payload={"weight_kg": 86.2},
        update_state=True,
    )
    strava = FakeStrava()

    result = await sync_zepp_weight_to_strava_once(
        zepp_client=FakeZepp([measurement(weight=86.25)]),
        strava_client=strava,
        state=state,
        start_date="2026-06-19",
        end_date="2026-06-21",
        dry_run=False,
        threshold_kg=0.1,
    )

    assert result.skipped == 1
    assert strava.weights == []
