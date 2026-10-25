"""Tests for the bronze event generator."""

import json
import random
from collections import Counter
from datetime import date, datetime, timedelta

import pytest

from generator import generate_events as gen

DAY = date(2026, 10, 20)
BRONZE_FIELDS = {
    "event_id",
    "user_id",
    "device_model",
    "os_version",
    "app_id",
    "event_type",
    "event_ts",
    "duration_ms",
    "country",
}


@pytest.fixture(autouse=True)
def pinned_settings(monkeypatch):
    """Pin settings: tests stay fast and ignore the shell environment."""
    monkeypatch.setattr(gen, "SEED", 42)
    monkeypatch.setattr(gen, "EVENTS_PER_DAY", 1_000)
    monkeypatch.setattr(gen, "NUM_USERS", 100)
    monkeypatch.setattr(gen, "DEFECT_RATE", 0.02)


@pytest.fixture
def users():
    return gen.make_users(random.Random(0))


def is_defective(event: dict, event_date: date) -> bool:
    """Recognize the defects injected by the generator."""
    if any(event[field] is None for field in gen.REQUIRED_FIELDS):
        return True
    try:
        ts = datetime.fromisoformat(event["event_ts"])
    except ValueError:
        return True
    if ts.date() > event_date:
        return True
    return event["duration_ms"] is not None and event["duration_ms"] < 0


def test_valid_event_matches_bronze_schema(users):
    rng = random.Random(0)
    for _ in range(200):
        event = gen.make_event(rng, DAY, users)
        assert set(event) == BRONZE_FIELDS
        assert datetime.fromisoformat(event["event_ts"]).date() == DAY
        # Only screen views and sessions have a duration.
        has_duration = event["event_type"] in gen.DURATION_PARAMS
        assert (event["duration_ms"] is not None) == has_duration
        assert not is_defective(event, DAY)


def test_same_seed_gives_same_batch(users):
    assert gen.build_batch(DAY, users) == gen.build_batch(DAY, users)


def test_different_seed_gives_different_batch(users, monkeypatch):
    first = gen.build_batch(DAY, users)
    monkeypatch.setattr(gen, "SEED", 43)
    assert gen.build_batch(DAY, users) != first


def test_batch_contains_late_events_from_previous_day_only(users):
    previous_day = DAY - timedelta(days=1)
    clean = [e for e in gen.build_batch(DAY, users) if not is_defective(e, DAY)]
    days = Counter(datetime.fromisoformat(e["event_ts"]).date() for e in clean)
    assert set(days) == {previous_day, DAY}
    assert 0 < days[previous_day] < 0.05 * len(clean)


def test_batch_contains_duplicates(users):
    batch = gen.build_batch(DAY, users)
    ids = Counter(e["event_id"] for e in batch if e["event_id"] is not None)
    assert any(count > 1 for count in ids.values())


@pytest.mark.parametrize("defect_rate", [0.0, 0.1])
def test_defect_rate_is_close_to_target(users, monkeypatch, defect_rate):
    monkeypatch.setattr(gen, "EVENTS_PER_DAY", 5_000)
    monkeypatch.setattr(gen, "DEFECT_RATE", defect_rate)
    on_time, late = gen.generate_day(DAY, users)
    events = on_time + late
    defects = sum(is_defective(e, DAY) for e in events)
    assert defects == pytest.approx(defect_rate * len(events), rel=0.2)


def test_negative_duration_only_on_events_with_duration():
    rng = random.Random(0)
    for _ in range(200):
        event = {"event_ts": "2026-10-20T12:00:00.000+00:00", "duration_ms": None}
        gen.inject_defect(rng, event)
        assert event["duration_ms"] is None


def test_partition_path_uses_batch_date():
    path = gen.partition_path(DAY)
    assert path.parent.name == "batch_date=2026-10-20"
    assert path.name == "events.jsonl"


def test_write_jsonl_round_trip_and_overwrite(tmp_path):
    events = [
        {"event_id": "a", "duration_ms": None},
        {"event_id": "b", "duration_ms": 5},
    ]
    path = tmp_path / "batch_date=2026-10-20" / "events.jsonl"
    gen.write_jsonl(events, path)
    gen.write_jsonl(events, path)  # a re-run must replace the file, not append
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == events


def test_read_env(monkeypatch):
    monkeypatch.setenv("GENERATOR_TEST_SETTING", "500")
    assert gen.read_env("GENERATOR_TEST_SETTING", 150_000) == 500
    monkeypatch.delenv("GENERATOR_TEST_SETTING")
    assert gen.read_env("GENERATOR_TEST_SETTING", 150_000) == 150_000
    monkeypatch.setenv("GENERATOR_TEST_SETTING", "abc")
    with pytest.raises(SystemExit):
        gen.read_env("GENERATOR_TEST_SETTING", 150_000)
