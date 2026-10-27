"""Bronze to silver: validate, deduplicate and type raw events."""

from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.common.cli import parse_run_args
from jobs.common.schemas import BRONZE_SCHEMA, CORRUPT_RECORD
from jobs.common.spark import get_spark

BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"
QUARANTINE_DIR = "data/quarantine"
# ISO 8601 with milliseconds and a UTC offset, as written by the generator.
EVENT_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
# Columns that are NOT NULL in silver.events.
REQUIRED_FIELDS = [
    "event_id",
    "user_id",
    "device_model",
    "app_id",
    "event_type",
    "event_ts",
]


def read_bronze(spark: SparkSession, path: str = BRONZE_DIR) -> DataFrame:
    """Read raw events with the explicit bronze schema.

    PERMISSIVE mode keeps every line: a line that cannot be parsed gets nulls
    and its raw text in the corrupt record column, so it can be quarantined
    instead of dropped. batch_date is added from the folder names.
    """
    return (
        spark.read.schema(BRONZE_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
        .json(path)
    )


def parse_event_ts(df: DataFrame) -> DataFrame:
    """Add the typed timestamp (parsed_ts) and the event_date partition key.

    try_to_timestamp returns null on a malformed value instead of failing the
    job: Spark 4 runs in ANSI mode, where to_timestamp raises. The raw event_ts
    string is kept so that a rejected row can be stored unchanged.
    """
    # The format must be a literal: a plain string would be read as a column name.
    parsed_ts = F.try_to_timestamp("event_ts", F.lit(EVENT_TS_FORMAT))
    # to_date uses the session timezone, forced to UTC by get_spark.
    event_date = F.to_date(parsed_ts)
    return df.withColumn("parsed_ts", parsed_ts).withColumn("event_date", event_date)


def batch_end() -> Column:
    """End of the batch day (next midnight, UTC): when the batch is complete."""
    return F.date_add("batch_date", 1).cast("timestamp")


def add_rejection_reason(df: DataFrame) -> DataFrame:
    """Add rejection_reason: the first rule a row breaks, or null if it is valid.

    Expects the columns added by parse_event_ts. Rules are checked in order,
    so a broken JSON line is reported as unparseable, not as a null field.
    """
    reason = F.when(F.col(CORRUPT_RECORD).isNotNull(), "unparseable_record")
    for field in REQUIRED_FIELDS:
        reason = reason.when(F.col(field).isNull(), f"null_{field}")

    # "Future" means after the end of the batch day, not after now(): the
    # result must not change when the job is re-run later (idempotency).
    reason = (
        reason.when(F.col("parsed_ts").isNull(), "malformed_timestamp")
        .when(F.col("parsed_ts") >= batch_end(), "future_timestamp")
        # A null duration gives a null comparison, which when() treats as false.
        .when(F.col("duration_ms") < 0, "negative_duration")
    )
    return df.withColumn("rejection_reason", reason)


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per event_id: the first received (smallest batch_date).

    At-least-once delivery sends some events twice. dropDuplicates would also
    keep one row per event_id, but gives no control over which one.
    """
    first_received = Window.partitionBy("event_id").orderBy("batch_date")
    return (
        df.withColumn("_row_number", F.row_number().over(first_received))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def build_silver(df: DataFrame) -> DataFrame:
    """Valid, deduplicated events with the silver.events columns and types."""
    valid = df.filter(F.col("rejection_reason").isNull())
    return deduplicate(valid).select(
        "event_id",
        "user_id",
        "device_model",
        "os_version",
        "app_id",
        "event_type",
        F.col("parsed_ts").alias("event_ts"),
        batch_end().alias("ingested_ts"),
        F.col("duration_ms").cast("int").alias("duration_ms"),
        "country",
        "event_date",
    )


def build_quarantine(df: DataFrame) -> DataFrame:
    """Rejected rows with the silver.rejected_events columns.

    raw_payload is the original event: the raw line when it could not be
    parsed, otherwise its bronze fields as JSON, nulls included (a null value
    is often the very reason of the rejection).
    """
    rejected = df.filter(F.col("rejection_reason").isNotNull())
    bronze_fields = [f.name for f in BRONZE_SCHEMA.fields if f.name != CORRUPT_RECORD]
    as_json = F.to_json(F.struct(*bronze_fields), {"ignoreNullFields": "false"})
    return rejected.select(
        F.coalesce(F.col(CORRUPT_RECORD), as_json).alias("raw_payload"),
        F.col("rejection_reason").alias("reason"),
        "batch_date",
        # Audit timestamp: the only value that changes when the job is re-run.
        F.current_timestamp().alias("rejected_ts"),
    )


def write_partitions(df: DataFrame, path: str, partition_column: str) -> None:
    """Write Parquet partitioned by a column, replacing only the partitions
    present in df (dynamic overwrite): re-running a day is safe."""
    (
        # One task per partition value: one file per folder, not many tiny ones.
        df.repartition(partition_column)
        .write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(partition_column)
        .parquet(path)
    )


def run(spark: SparkSession, run_date: date, lookback_days: int) -> None:
    """Process the batch received on run_date.

    Events arrive up to lookback_days late, so the event_date partitions from
    run_date - lookback_days to run_date are rebuilt in full from every batch
    that can contain them. Older partitions are already complete: they are
    left untouched.
    """
    window_start = run_date - timedelta(days=lookback_days)
    # Late events of a day can sit in the next lookback_days batches. On a
    # daily run they do not exist yet; on a re-run or a backfill they do, and
    # skipping them would truncate the rebuilt days. Reading them makes the
    # result independent of the order in which dates are processed.
    last_batch = run_date + timedelta(days=lookback_days)
    in_window = F.col("batch_date").between(window_start, last_batch)
    # Used twice (silver and quarantine): keep it in memory instead of
    # reading and parsing the JSON files again.
    df = add_rejection_reason(parse_event_ts(read_bronze(spark).filter(in_window)))
    df.cache()

    # Late events of days before the window would overwrite complete
    # partitions with a handful of rows: keep only the rebuilt days.
    silver = build_silver(df).filter(
        F.col("event_date").between(window_start, run_date)
    )
    write_partitions(silver, SILVER_DIR, "event_date")

    # Rejections of earlier batches were stored when those batches arrived.
    quarantine = build_quarantine(df.filter(F.col("batch_date") == run_date))
    write_partitions(quarantine, QUARANTINE_DIR, "batch_date")
    df.unpersist()


def main() -> None:
    args = parse_run_args(__doc__)
    spark = get_spark("bronze_to_silver")
    run(spark, args.date, args.lookback_days)
    window_start = args.date - timedelta(days=args.lookback_days)
    print(
        f"Processed batch {args.date}, rebuilt event dates {window_start}..{args.date}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
