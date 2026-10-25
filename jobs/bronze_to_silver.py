"""Bronze to silver: validate, deduplicate and type raw events."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.common.schemas import BRONZE_SCHEMA, CORRUPT_RECORD

BRONZE_DIR = "data/bronze"
# ISO 8601 with milliseconds and a UTC offset, as written by the generator.
EVENT_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"


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
