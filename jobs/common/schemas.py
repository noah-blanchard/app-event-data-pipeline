"""Explicit schemas: declared once in code, never inferred from the data."""

from pyspark.sql.types import LongType, StringType, StructField, StructType

CORRUPT_RECORD = "_corrupt_record"

# Bronze is read as close to the raw JSON as possible. event_ts stays a string
# because some values are malformed: it is parsed later, so that bad values can
# be rejected with a reason instead of silently becoming null.
BRONZE_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("user_id", LongType()),
        StructField("device_model", StringType()),
        StructField("os_version", StringType()),
        StructField("app_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_ts", StringType()),
        StructField("duration_ms", LongType()),
        StructField("country", StringType()),
        # Raw line, set when it is not valid JSON or a value has the wrong type.
        StructField(CORRUPT_RECORD, StringType()),
    ]
)
