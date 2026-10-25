"""Bronze to silver: validate, deduplicate and type raw events."""

from pyspark.sql import DataFrame, SparkSession

from jobs.common.schemas import BRONZE_SCHEMA, CORRUPT_RECORD

BRONZE_DIR = "data/bronze"


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
