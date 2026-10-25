"""Shared Spark session: every job gets the same settings."""

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """Create (or reuse) a local Spark session with the pipeline settings."""
    builder = (
        SparkSession.builder.appName(app_name)
        # One process, one thread per CPU core.
        .master("local[*]")
        # Parse and display timestamps in UTC, whatever the machine's timezone.
        .config("spark.sql.session.timeZone", "UTC")
    )
    # PostgreSQL JDBC driver, set by the Docker image. Elsewhere (CI, tests)
    # Spark runs without it: only the warehouse loads need it.
    jdbc_jar = os.environ.get("POSTGRES_JDBC_JAR")
    if jdbc_jar:
        builder = builder.config("spark.jars", jdbc_jar)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
