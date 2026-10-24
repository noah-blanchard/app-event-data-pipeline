"""Generate synthetic mobile app usage events as JSON lines (bronze layer)."""

import json
import random
import uuid
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

BRONZE_DIR = Path("data/bronze")
SEED = 42
EVENTS_PER_DAY = 1_000
START_DATE = date(2026, 10, 20)
DAYS = 3
MS_PER_DAY = 24 * 60 * 60 * 1000


def make_event(rng: random.Random, event_date: date) -> dict:
    """Build one valid event with every field of the bronze schema."""
    day_start = datetime.combine(event_date, time.min, tzinfo=UTC)
    event_ts = day_start + timedelta(milliseconds=rng.randrange(MS_PER_DAY))
    return {
        # Derived from the seeded RNG: uuid4() reads OS entropy and changes every run.
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "user_id": 42,
        "device_model": "Redmi Note 13",
        "os_version": "Android 14",
        "app_id": "com.example.weather",
        "event_type": "app_close",
        "event_ts": event_ts.isoformat(timespec="milliseconds"),
        "duration_ms": 42_000,
        "country": "FR",
    }


def partition_path(event_date: date) -> Path:
    """Hive-style partition: data/bronze/event_date=YYYY-MM-DD/events.jsonl."""
    return BRONZE_DIR / f"event_date={event_date.isoformat()}" / "events.jsonl"


def write_jsonl(events: list[dict], path: Path) -> None:
    """Write events as JSON lines: one JSON object per line."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Mode "w" overwrites: re-running a day replaces its partition (idempotent).
    # Force "\n" so the file is byte-identical on Windows and Linux.
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


def main() -> None:
    for offset in range(DAYS):
        event_date = START_DATE + timedelta(days=offset)
        # One RNG per day: a partition depends only on (SEED, date), so
        # regenerating a single day gives the same file as a full run.
        rng = random.Random(f"{SEED}-{event_date.isoformat()}")
        events = [make_event(rng, event_date) for _ in range(EVENTS_PER_DAY)]
        path = partition_path(event_date)
        write_jsonl(events, path)
        print(f"Wrote {len(events)} events to {path}")


if __name__ == "__main__":
    main()
