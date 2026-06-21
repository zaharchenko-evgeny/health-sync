from datetime import date

from health_sync import mappings


def test_zepp_body_to_garmin_maps_supported_fields():
    payload = mappings.zepp_body_to_garmin(
        {
            "timestamp": "2026-06-21T07:30:00",
            "weight_kg": 86.2,
            "body_fat_pct": 20.1,
            "water_pct": 55.2,
            "muscle_mass_kg": 65.0,
            "bone_mass_kg": 3.1,
            "basal_metabolism_kcal": 1800,
            "metabolic_age": 42,
            "visceral_fat_score": 9,
            "bmi": 25.1,
        }
    )

    assert payload == {
        "timestamp": "2026-06-21T07:30:00",
        "weight": 86.2,
        "percent_fat": 20.1,
        "percent_hydration": 55.2,
        "muscle_mass": 65.0,
        "bone_mass": 3.1,
        "basal_met": 1800.0,
        "metabolic_age": 42.0,
        "visceral_fat_rating": 9.0,
        "bmi": 25.1,
    }


def test_yazio_summary_to_daily_totals_sums_meals():
    totals = mappings.yazio_summary_to_daily_totals(
        {
            "meals": {
                "breakfast": {
                    "nutrients": {
                        "energy.energy": 400,
                        "nutrient.carb": 50,
                        "nutrient.fat": 10,
                        "nutrient.protein": 20,
                    }
                },
                "lunch": {
                    "nutrients": {
                        "energy.energy": 800,
                        "nutrient.carb": 70,
                        "nutrient.fat": 25,
                        "nutrient.protein": 45,
                    }
                },
            }
        }
    )

    assert totals == {"calories": 1200.0, "carbs": 120.0, "protein": 65.0, "fat": 35.0}


def test_hash_is_stable_for_equivalent_payloads():
    left = {"b": 2, "a": 1}
    right = {"a": 1, "b": 2}

    assert mappings.payload_hash(left) == mappings.payload_hash(right)


def test_rolling_date_range_uses_lookback_inclusive():
    assert mappings.rolling_date_range(3, "Europe/Berlin", date(2026, 6, 21)) == (
        "2026-06-19",
        "2026-06-21",
    )
