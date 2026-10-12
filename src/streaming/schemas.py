from pyspark.sql.types import DoubleType, StringType, StructField, StructType


TRANSACTION_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("country", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

VALID_STATUSES = ["approved", "declined", "reversed"]
