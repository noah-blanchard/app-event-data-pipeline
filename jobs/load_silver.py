"""Load silver Parquet into PostgreSQL: the event dates rebuilt by a batch."""

from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.bronze_to_silver import SILVER_DIR
from jobs.common.cli import parse_run_args
from jobs.common.spark import get_spark
from jobs.common.warehouse import create_daily_partitions, replace_rows


def run(spark: SparkSession, run_date: date, lookback_days: int) -> int:
    """Copy the event dates rebuilt by bronze to silver for run_date.

    Same window as bronze to silver: these are the only Parquet partitions
    that changed, so they are the only rows to replace in PostgreSQL.
    """
    window_start = run_date - timedelta(days=lookback_days)
    # Filtering on the partition column: Spark only reads those folders.
    events = spark.read.parquet(SILVER_DIR).filter(
        F.col("event_date").between(window_start, run_date)
    )
    create_daily_partitions("silver.events", window_start, run_date)
    return replace_rows(events, "silver.events", "event_date", window_start, run_date)


def main() -> None:
    args = parse_run_args(__doc__)
    spark = get_spark("load_silver")
    rows = run(spark, args.date, args.lookback_days)
    window_start = args.date - timedelta(days=args.lookback_days)
    print(f"Loaded {rows} rows into silver.events for {window_start}..{args.date}")
    spark.stop()


if __name__ == "__main__":
    main()
