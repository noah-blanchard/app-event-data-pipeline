"""Bronze to silver: validate, deduplicate and type raw events."""

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.common.schemas import BRONZE_SCHEMA, CORRUPT_RECORD

BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"
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
