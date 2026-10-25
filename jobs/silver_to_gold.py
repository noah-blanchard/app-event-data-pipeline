"""Silver to gold: compute the daily business metrics and load them."""

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.bronze_to_silver import SILVER_DIR
from jobs.common.cli import parse_run_args
from jobs.common.spark import get_spark
from jobs.common.warehouse import replace_rows


def daily_active_users(events: DataFrame, start: date, end: date) -> DataFrame:
    """One row per event_date from start to end: dau, new and returning users.

    events must hold the whole silver history: a user is new on the day of
    their first event ever, which can be before the window. On the first day
    of data every user is new (cold start).
    """
    first_seen = events.groupBy("user_id").agg(
        F.min("event_date").alias("first_event_date")
    )
    is_first_day = F.col("event_date") == F.col("first_event_date")
    return (
        events.filter(F.col("event_date").between(start, end))
        .join(first_seen, "user_id")
        .groupBy("event_date")
        .agg(
            F.count_distinct("user_id").alias("dau"),
            # when() without otherwise() gives null, which count_distinct skips.
            F.count_distinct(F.when(is_first_day, F.col("user_id"))).alias("new_users"),
        )
        .withColumn("returning_users", F.col("dau") - F.col("new_users"))
    )


def run(spark: SparkSession, run_date: date, lookback_days: int) -> None:
    """Rebuild the gold rows of the event dates rebuilt in silver for run_date."""
    window_start = run_date - timedelta(days=lookback_days)
    events = spark.read.parquet(SILVER_DIR)
    replace_rows(
        daily_active_users(events, window_start, run_date),
        "gold.daily_active_users",
        "event_date",
        window_start,
        run_date,
    )


def main() -> None:
    args = parse_run_args(__doc__)
    spark = get_spark("silver_to_gold")
    run(spark, args.date, args.lookback_days)
    window_start = args.date - timedelta(days=args.lookback_days)
    print(f"Rebuilt gold tables for {window_start}..{args.date}")
    spark.stop()


if __name__ == "__main__":
    main()
