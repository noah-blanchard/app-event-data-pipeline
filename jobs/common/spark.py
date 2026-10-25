"""Shared Spark session: every job gets the same settings."""

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """Create (or reuse) a local Spark session with the pipeline settings."""
    spark = (
        SparkSession.builder.appName(app_name)
        # One process, one thread per CPU core.
        .master("local[*]")
        # Parse and display timestamps in UTC, whatever the machine's timezone.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
