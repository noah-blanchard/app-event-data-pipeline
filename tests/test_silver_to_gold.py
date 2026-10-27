"""Tests for the gold aggregations, on a tiny hand-made silver history."""

from datetime import date

import pytest
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from jobs.silver_to_gold import (
    app_usage_daily,
    country_daily,
    daily_active_users,
    device_daily,
)

D0, D1, D2 = date(2026, 10, 19), date(2026, 10, 20), date(2026, 10, 21)
# Only the silver columns the aggregations read.
SCHEMA = StructType(
    [
        StructField("user_id", LongType()),
        StructField("device_model", StringType()),
        StructField("country", StringType()),
        StructField("app_id", StringType()),
        StructField("event_type", StringType()),
        StructField("duration_ms", IntegerType()),
        StructField("event_date", DateType()),
    ]
)
# user_id: (device_model, country). User 3 has no country.
USERS = {1: ("iPhone 15", "FR"), 2: ("Pixel 8", "US"), 3: ("Pixel 8", None)}
# (user_id, app_id, event_type, duration_ms, event_date)
EVENTS = [
    # D0 is before the window: user 2 is not new on D1.
    (2, "app02", "click", None, D0),
    # D1: users 1 (first time) and 2.
    (1, "app01", "app_open", None, D1),
    (1, "app01", "app_close", 60_000, D1),
    (2, "app02", "click", None, D1),
    (2, "app02", "crash", None, D1),
    # D2: users 1 and 2 again, user 3 for the first time.
    (1, "app01", "app_close", 120_000, D2),
    (1, "app01", "app_close", 60_000, D2),
    (2, "app02", "click", None, D2),
    (3, "app02", "app_close", 30_000, D2),
    (3, "app02", "crash", None, D2),
]


@pytest.fixture(scope="module")
def silver(spark):
    rows = [
        (user_id, *USERS[user_id], app_id, event_type, duration_ms, event_date)
        for user_id, app_id, event_type, duration_ms, event_date in EVENTS
    ]
    return spark.createDataFrame(rows, SCHEMA)


def by_key(df, *keys):
    """Collect df as {key values: row as dict}."""
    return {tuple(row[k] for k in keys): row.asDict() for row in df.collect()}


def test_daily_active_users(silver):
    rows = by_key(daily_active_users(silver, D1, D2), "event_date")
    # D0 is only read for the history, not reported.
    assert set(rows) == {(D1,), (D2,)}
    assert rows[(D1,)] == {
        "event_date": D1,
        "dau": 2,
        "new_users": 1,
        "returning_users": 1,
    }
    assert rows[(D2,)] == {
        "event_date": D2,
        "dau": 3,
        "new_users": 1,
        "returning_users": 2,
    }


def test_app_usage_daily(silver):
    rows = by_key(app_usage_daily(silver, D1, D2), "event_date", "app_id")
    assert set(rows) == {(D1, "app01"), (D1, "app02"), (D2, "app01"), (D2, "app02")}

    day1_app01 = rows[(D1, "app01")]
    assert day1_app01["active_users"] == 1
    assert day1_app01["sessions"] == 1
    assert day1_app01["total_duration_ms"] == 60_000

    # Two sessions: the average is not truncated to an integer.
    day2_app01 = rows[(D2, "app01")]
    assert day2_app01["sessions"] == 2
    assert day2_app01["total_duration_ms"] == 180_000
    assert day2_app01["avg_session_ms"] == 90_000.0

    # Active users but no session: no average instead of a division by zero.
    day1_app02 = rows[(D1, "app02")]
    assert day1_app02["sessions"] == 0
    assert day1_app02["total_duration_ms"] == 0
    assert day1_app02["avg_session_ms"] is None

    assert rows[(D2, "app02")]["active_users"] == 2


def test_device_daily_crash_rate(silver):
    rows = by_key(device_daily(silver, D1, D2), "event_date", "device_model")
    assert rows[(D1, "iPhone 15")]["crash_rate"] == 0.0
    assert rows[(D1, "Pixel 8")]["crash_rate"] == 1.0
    # One crash for two Pixel 8 users.
    day2_pixel = rows[(D2, "Pixel 8")]
    assert day2_pixel["active_users"] == 2
    assert day2_pixel["crash_count"] == 1
    assert day2_pixel["crash_rate"] == 0.5


def test_country_daily_keeps_unknown_country(silver):
    rows = by_key(country_daily(silver, D1, D2), "event_date", "country")
    assert rows[(D2, "FR")]["sessions"] == 2
    assert rows[(D2, "US")]["sessions"] == 0
    # The user without a country is counted in its own row.
    assert rows[(D2, None)]["active_users"] == 1
    assert rows[(D2, None)]["sessions"] == 1
