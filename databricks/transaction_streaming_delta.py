import os

from pyspark.sql.functions import avg, col, concat_ws, count, expr, from_json, max as spark_max, sum as spark_sum, to_timestamp, window

from streaming.schemas import TRANSACTION_SCHEMA


catalog = os.getenv("DATABRICKS_CATALOG", "main")
schema = os.getenv("DATABRICKS_SCHEMA", "streaming_analytics")
checkpoint_base = os.getenv("CHECKPOINT_BASE", "dbfs:/checkpoints/real-time-transactions")

spark.sql(f"create schema if not exists {catalog}.{schema}")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    .option("subscribe", os.getenv("KAFKA_TOPIC", "transactions.v1"))
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("value").cast("string").alias("raw_payload"),
    )
    .withColumn("event", from_json(col("raw_payload"), TRANSACTION_SCHEMA))
    .select("*", "event.*")
    .drop("event")
    .withColumn("event_ts", to_timestamp("event_time"))
    .withColumn(
        "error_reason",
        concat_ws(
            ";",
            expr("case when event_id is null or event_id = '' then 'missing_event_id' end"),
            expr("case when account_id is null or account_id = '' then 'missing_account_id' end"),
            expr("case when event_ts is null then 'invalid_event_time' end"),
            expr("case when amount is null or amount <= 0 then 'invalid_amount' end"),
            expr("case when status not in ('approved','declined','reversed') then 'invalid_status' end"),
        ),
    )
)

valid = parsed.filter(col("error_reason") == "").withWatermark("event_ts", "10 minutes").dropDuplicates(["event_id"])
invalid = parsed.filter(col("error_reason") != "")

valid.writeStream.format("delta").outputMode("append").option(
    "checkpointLocation", f"{checkpoint_base}/silver_transactions"
).toTable(f"{catalog}.{schema}.silver_transactions")

invalid.writeStream.format("delta").outputMode("append").option(
    "checkpointLocation", f"{checkpoint_base}/invalid_transactions"
).toTable(f"{catalog}.{schema}.invalid_transactions")

(
    valid.groupBy(window(col("event_ts"), "5 minutes"), col("country"), col("status"))
    .agg(
        count("*").alias("transaction_count"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount"),
        spark_max("event_ts").alias("max_event_time"),
    )
    .writeStream.format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{checkpoint_base}/gold_transaction_windows")
    .toTable(f"{catalog}.{schema}.gold_transaction_windows")
)

spark.streams.awaitAnyTermination()
