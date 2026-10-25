"""Generate synthetic mobile app usage events as JSON lines (bronze layer)."""

import argparse
import json
import math
import os
import random
import sys
import uuid
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path


def read_env[T: (int, float)](name: str, default: T) -> T:
    """Read a numeric setting from the environment, falling back to a default."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return type(default)(raw)
    except ValueError:
        sys.exit(f"{name} must be a {type(default).__name__}, got {raw!r}")


# Settings that describe the environment come from environment variables.
SEED = read_env("SEED", 42)
EVENTS_PER_DAY = read_env("EVENTS_PER_DAY", 150_000)
NUM_USERS = read_env("NUM_USERS", 20_000)
DEFECT_RATE = read_env("DEFECT_RATE", 0.02)

BRONZE_DIR = Path("data/bronze")
NUM_APPS = 50
# Delivery issues, separate from corrupted content.
DUPLICATE_RATE = 0.01
LATE_RATE = 0.02
MS_PER_HOUR = 60 * 60 * 1000

# Fields that must never be null. os_version, duration_ms and country are
# nullable by design, so a null there is not a defect.
REQUIRED_FIELDS = [
    "event_id",
    "user_id",
    "device_model",
    "app_id",
    "event_type",
    "event_ts",
]

# Relative activity per hour of the day (UTC): quiet nights, a lunch peak
# and a bigger evening peak.
HOURLY_WEIGHTS = [
    3, 2, 1, 1, 1, 2,  # 00-05: night
    4, 7, 9, 8, 7, 8,  # 06-11: morning
    11, 10, 8, 7, 7, 8,  # 12-17: lunch peak, afternoon
    10, 12, 13, 12, 9, 5,  # 18-23: evening peak
]  # fmt: skip

# Relative frequency of each event type: crashes are rare.
EVENT_TYPE_WEIGHTS = {
    "screen_view": 50,
    "click": 30,
    "app_open": 10,
    "app_close": 9.5,
    "crash": 0.5,
}
EVENT_TYPES = list(EVENT_TYPE_WEIGHTS)
EVENT_WEIGHTS = list(EVENT_TYPE_WEIGHTS.values())

# Log-normal duration per event type, as (median_ms, sigma). A larger sigma
# gives a longer tail. Other event types have no duration.
DURATION_PARAMS = {
    "screen_view": (8_000, 1.0),  # time spent on a screen
    "app_close": (120_000, 1.2),  # session length
}

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
    # Pick the hour from the daily profile, then a uniform instant within it.
    hour = rng.choices(range(24), weights=HOURLY_WEIGHTS)[0]
    event_ts = day_start + timedelta(
        hours=hour, milliseconds=rng.randrange(MS_PER_HOUR)
    )
    event_type = rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    return {
        # Derived from the seeded RNG: uuid4() reads OS entropy and changes every run.
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "user_id": user["user_id"],
        "device_model": user["device_model"],
        "os_version": user["os_version"],
        "app_id": rng.choices(APP_IDS, weights=APP_WEIGHTS)[0],
        "event_type": event_type,
        "event_ts": event_ts.isoformat(timespec="milliseconds"),
        "duration_ms": make_duration_ms(rng, event_type),
        "country": user["country"],
    }


def make_duration_ms(rng: random.Random, event_type: str) -> int | None:
    """Heavy-tailed duration for screen views and sessions, None otherwise."""
    if event_type not in DURATION_PARAMS:
        return None
    median_ms, sigma = DURATION_PARAMS[event_type]
    # The median of a log-normal is exp(mu), so mu = log(median).
    return round(rng.lognormvariate(math.log(median_ms), sigma))


def inject_defect(rng: random.Random, event: dict) -> None:
    """Corrupt the event in place with one defect the quality layer must catch."""
    defects = ["null_required_field", "malformed_timestamp", "future_timestamp"]
    # A negative duration only makes sense on events that have a duration.
    duration_ms = event["duration_ms"]
    if duration_ms is not None:
        defects.append("negative_duration")

    defect = rng.choice(defects)
    if defect == "null_required_field":
        event[rng.choice(REQUIRED_FIELDS)] = None
    elif defect == "malformed_timestamp":
        ts = datetime.fromisoformat(event["event_ts"])
        event["event_ts"] = ts.strftime("%d/%m/%Y %H:%M:%S")
    elif defect == "future_timestamp":
        # A device with a broken clock: months to years ahead.
        ts = datetime.fromisoformat(event["event_ts"])
        ts += timedelta(days=rng.randint(30, 3_650))
        event["event_ts"] = ts.isoformat(timespec="milliseconds")
    elif defect == "negative_duration" and duration_ms is not None:
        event["duration_ms"] = -duration_ms


def generate_day(event_date: date, users: list[dict]) -> tuple[list, list]:
    """Generate the events that happened on a day, split into (on_time, late).

    Late events are delivered in the next day's batch.
    """
    # One RNG per day: the result depends only on (SEED, date), so a day can
    # be regenerated on its own and gives the same events as a full run.
    rng = random.Random(f"{SEED}-{event_date.isoformat()}")
    on_time, late = [], []
    for _ in range(EVENTS_PER_DAY):
        event = make_event(rng, event_date, users)
        if rng.random() < DEFECT_RATE:
            inject_defect(rng, event)
        if rng.random() < LATE_RATE:
            late.append(event)
        else:
            on_time.append(event)
    return on_time, late


def build_batch(batch_date: date, users: list[dict]) -> list[dict]:
    """Build the batch received on a day: its on-time events, the late events
    of the previous day and some duplicates, in no particular order."""
    on_time, _ = generate_day(batch_date, users)
    _, late_from_previous_day = generate_day(batch_date - timedelta(days=1), users)
    batch = on_time + late_from_previous_day

    rng = random.Random(f"{SEED}-batch-{batch_date.isoformat()}")
    # At-least-once delivery: some events are sent twice, unchanged.
    batch += [event for event in batch if rng.random() < DUPLICATE_RATE]
    rng.shuffle(batch)
    return batch


def partition_path(batch_date: date) -> Path:
    """Hive-style partition: data/bronze/batch_date=YYYY-MM-DD/events.jsonl.

    Bronze stores events as received, so it is partitioned by arrival day.
    """
    return BRONZE_DIR / f"batch_date={batch_date.isoformat()}" / "events.jsonl"


def write_jsonl(events: list[dict], path: Path) -> None:
    """Write events as JSON lines: one JSON object per line."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Mode "w" overwrites: re-running a day replaces its partition (idempotent).
    # Force "\n" so the file is byte-identical on Windows and Linux.
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


def parse_args() -> argparse.Namespace:
    """Read the dates to generate from the command line: they describe this run."""
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=yesterday,
        help="first batch date, YYYY-MM-DD (default: yesterday, UTC)",
    )
    parser.add_argument(
        "--days", type=int, default=1, help="number of daily batches (default: 1)"
    )
    args = parser.parse_args()
    if args.days < 1:
        parser.error("--days must be at least 1")
    return args


def check_settings() -> None:
    """Stop early with a clear message instead of generating wrong data."""
    if EVENTS_PER_DAY < 1 or NUM_USERS < 1:
        sys.exit("EVENTS_PER_DAY and NUM_USERS must be at least 1")
    if not 0 <= DEFECT_RATE <= 1:
        sys.exit(f"DEFECT_RATE must be between 0 and 1, got {DEFECT_RATE}")


def main() -> None:
    args = parse_args()
    check_settings()
    # The user pool has its own seed, independent of the day: a user keeps
    # the same profile across all partitions.
    users = make_users(random.Random(f"{SEED}-users"))
    for offset in range(args.days):
        batch_date = args.start_date + timedelta(days=offset)
        events = build_batch(batch_date, users)
        path = partition_path(batch_date)
        write_jsonl(events, path)
        print(f"Wrote {len(events)} events to {path}")


if __name__ == "__main__":
    main()
