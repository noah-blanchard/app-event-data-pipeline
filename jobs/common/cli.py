"""Command-line arguments shared by the jobs that process one batch date."""

import argparse
import os
from datetime import UTC, date, datetime, timedelta


def parse_run_args(description: str | None) -> argparse.Namespace:
    """Parse --date and --lookback-days: the batch to process and its window."""
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=yesterday,
        help="batch date to process, YYYY-MM-DD (default: yesterday, UTC)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=int(os.environ.get("LOOKBACK_DAYS", "2")),
        help="days of late events to reprocess (default: $LOOKBACK_DAYS or 2)",
    )
    args = parser.parse_args()
    if args.lookback_days < 0:
        parser.error("--lookback-days must be 0 or more")
    return args
