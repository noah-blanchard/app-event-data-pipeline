"""Generate synthetic mobile app usage events as JSON lines (bronze layer)."""

import json
import random
import uuid
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

OUTPUT_PATH = Path("data/bronze/events.jsonl")
SEED = 42
EVENT_COUNT = 1_000
EVENT_DATE = date(2026, 10, 20)
MS_PER_DAY = 24 * 60 * 60 * 1000


def make_event(rng: random.Random) -> dict:
    """Build one valid event with every field of the bronze schema."""
    day_start = datetime.combine(EVENT_DATE, time.min, tzinfo=UTC)
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


def write_jsonl(events: list[dict], path: Path) -> None:
    """Write events as JSON lines: one JSON object per line."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Force "\n" so the file is byte-identical on Windows and Linux.
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


def main() -> None:
    rng = random.Random(SEED)
    events = [make_event(rng) for _ in range(EVENT_COUNT)]
    write_jsonl(events, OUTPUT_PATH)
    print(f"Wrote {len(events)} events to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
