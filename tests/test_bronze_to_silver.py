"""Tests for the bronze to silver transformations, on small in-memory DataFrames."""

import json
from datetime import date, datetime

import pytest
from pyspark.sql.types import DateType, StructField, StructType

from jobs.bronze_to_silver import (
    add_rejection_reason,
    build_quarantine,
    build_silver,
    deduplicate,
    parse_event_ts,
)
from jobs.common.schemas import BRONZE_SCHEMA

BATCH_DATE = date(2026, 10, 20)
SCHEMA = StructType([*BRONZE_SCHEMA.fields, StructField("batch_date", DateType())])
VALID_EVENT = {
    "event_id": "e1",
    "user_id": 1,
    "device_model": "Pixel 8",
    "os_version": "Android 15",
    "app_id": "com.example.app01",
    "event_type": "screen_view",
    "event_ts": "2026-10-20T12:00:00.000+00:00",
    "duration_ms": 8_000,
    "country": "FR",
    "_corrupt_record": None,
    "batch_date": BATCH_DATE,
}


def bronze(spark, *overrides: dict):
    """A bronze DataFrame with one valid event per override, patched by it."""
    rows = [{**VALID_EVENT, **override} for override in overrides]
    return spark.createDataFrame(
        [tuple(row[field.name] for field in SCHEMA.fields) for row in rows], SCHEMA
    )


def tagged(spark, *overrides: dict):
    return add_rejection_reason(parse_event_ts(bronze(spark, *overrides)))


@pytest.mark.parametrize(
    ("event_ts", "expected_date"),
    [
        ("2026-10-20T12:00:00.000+00:00", date(2026, 10, 20)),
        # 23:30 at UTC-2 is 01:30 UTC the next day.
        ("2026-10-20T23:30:00.000-02:00", date(2026, 10, 21)),
        ("20/10/2026 12:00:00", None),
    ],
)
def test_parse_event_ts(spark, event_ts, expected_date):
    row = parse_event_ts(bronze(spark, {"event_ts": event_ts})).first()
    assert row["event_date"] == expected_date
    assert (row["parsed_ts"] is None) == (expected_date is None)


@pytest.mark.parametrize(
    ("override", "expected_reason"),
    [
        ({}, None),
        (
            {"_corrupt_record": '{"event_id": "e1", ', "event_id": None},
            "unparseable_record",
        ),
        ({"user_id": None}, "null_user_id"),
        ({"event_ts": None}, "null_event_ts"),
        ({"event_ts": "20/10/2026 12:00:00"}, "malformed_timestamp"),
        # The batch ends at the next midnight UTC.
        ({"event_ts": "2026-10-20T23:59:59.999+00:00"}, None),
        ({"event_ts": "2026-10-21T00:00:00.000+00:00"}, "future_timestamp"),
        ({"duration_ms": -1}, "negative_duration"),
        # No duration is legitimate for a click.
        ({"event_type": "click", "duration_ms": None}, None),
    ],
)
def test_rejection_reason(spark, override, expected_reason):
    assert tagged(spark, override).first()["rejection_reason"] == expected_reason


def test_deduplicate_keeps_first_received(spark):
    df = bronze(
        spark,
        {"event_id": "dup", "user_id": 2, "batch_date": date(2026, 10, 21)},
        {"event_id": "dup", "user_id": 1, "batch_date": date(2026, 10, 20)},
        {"event_id": "single"},
    )
    rows = {row["event_id"]: row for row in deduplicate(df).collect()}
    assert set(rows) == {"dup", "single"}
    assert rows["dup"]["batch_date"] == date(2026, 10, 20)
    assert rows["dup"]["user_id"] == 1


def test_build_silver_keeps_valid_typed_events(spark):
    silver = build_silver(tagged(spark, {}, {"event_id": "bad", "user_id": None}))
    assert silver.columns == [
        "event_id",
        "user_id",
        "device_model",
        "os_version",
        "app_id",
        "event_type",
        "event_ts",
        "ingested_ts",
        "duration_ms",
        "country",
        "event_date",
    ]
    assert dict(silver.dtypes)["duration_ms"] == "int"
    [row] = silver.collect()
    assert row["event_id"] == "e1"
    assert row["ingested_ts"] == datetime(2026, 10, 21)
    assert row["event_date"] == BATCH_DATE


def test_build_quarantine_keeps_the_original_event(spark):
    broken_line = '{"event_id": "e3", '
    quarantine = build_quarantine(
        tagged(
            spark,
            {},
            {"event_id": "e2", "user_id": None},
            {"_corrupt_record": broken_line, "event_id": None},
        )
    )
    rows = {row["reason"]: row for row in quarantine.collect()}
    assert set(rows) == {"null_user_id", "unparseable_record"}
    # The null value that caused the rejection is kept in the payload.
    assert json.loads(rows["null_user_id"]["raw_payload"])["user_id"] is None
    assert rows["unparseable_record"]["raw_payload"] == broken_line
    assert rows["null_user_id"]["batch_date"] == BATCH_DATE
