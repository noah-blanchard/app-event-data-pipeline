"""Generate synthetic mobile app usage events as JSON lines (bronze layer)."""

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

OUTPUT_PATH = Path("data/bronze/events.jsonl")


def make_event() -> dict:
    """Build one valid event with every field of the bronze schema."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": 42,
        "device_model": "Redmi Note 13",
        "os_version": "Android 14",
        "app_id": "com.example.weather",
        "event_type": "app_close",
        "event_ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
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
    write_jsonl([make_event()], OUTPUT_PATH)
    print(f"Wrote 1 event to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
