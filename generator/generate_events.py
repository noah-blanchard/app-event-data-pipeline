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
NUM_USERS = 20_000
NUM_APPS = 50
MS_PER_DAY = 24 * 60 * 60 * 1000

# Each device model only runs the OS versions it supports.
DEVICE_OS_VERSIONS = {
    "iPhone 15": ["iOS 17.6", "iOS 18.1"],
    "iPhone 13": ["iOS 16.7", "iOS 17.6", "iOS 18.1"],
    "Galaxy S24": ["Android 14", "Android 15"],
    "Galaxy A54": ["Android 13", "Android 14"],
    "Redmi Note 13": ["Android 13", "Android 14"],
    "Pixel 8": ["Android 14", "Android 15"],
}
DEVICE_MODELS = list(DEVICE_OS_VERSIONS)
COUNTRIES = ["FR", "US", "DE", "GB", "CA", "BR", "IN", "JP"]

# Rank 1 is the most popular app.
APP_IDS = [f"com.example.app{rank:02d}" for rank in range(1, NUM_APPS + 1)]
# Zipf weights: the app of rank r is picked proportionally to 1/r.
APP_WEIGHTS = [1 / rank for rank in range(1, NUM_APPS + 1)]


def make_users(rng: random.Random) -> list[dict]:
    """Build the user pool: each user keeps the same device, OS and country."""
    users = []
    for user_id in range(1, NUM_USERS + 1):
        device_model = rng.choice(DEVICE_MODELS)
        users.append(
            {
                "user_id": user_id,
                "device_model": device_model,
                "os_version": rng.choice(DEVICE_OS_VERSIONS[device_model]),
                "country": rng.choice(COUNTRIES),
            }
        )
    return users


def make_event(rng: random.Random, event_date: date, users: list[dict]) -> dict:
    """Build one valid event with every field of the bronze schema."""
    user = rng.choice(users)
    day_start = datetime.combine(event_date, time.min, tzinfo=UTC)
    event_ts = day_start + timedelta(milliseconds=rng.randrange(MS_PER_DAY))
    return {
        # Derived from the seeded RNG: uuid4() reads OS entropy and changes every run.
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "user_id": user["user_id"],
        "device_model": user["device_model"],
        "os_version": user["os_version"],
        "app_id": rng.choices(APP_IDS, weights=APP_WEIGHTS)[0],
        "event_type": "app_close",
        "event_ts": event_ts.isoformat(timespec="milliseconds"),
        "duration_ms": 42_000,
        "country": user["country"],
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
    # The user pool has its own seed, independent of the day: a user keeps
    # the same profile across all partitions.
    users = make_users(random.Random(f"{SEED}-users"))
    for offset in range(DAYS):
        event_date = START_DATE + timedelta(days=offset)
        # One RNG per day: a partition depends only on (SEED, date), so
        # regenerating a single day gives the same file as a full run.
        rng = random.Random(f"{SEED}-{event_date.isoformat()}")
        events = [make_event(rng, event_date, users) for _ in range(EVENTS_PER_DAY)]
        path = partition_path(event_date)
        write_jsonl(events, path)
        print(f"Wrote {len(events)} events to {path}")


if __name__ == "__main__":
    main()
