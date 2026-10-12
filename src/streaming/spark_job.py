import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    current_timestamp,
    expr,
    from_json,
    lit,
    max as spark_max,
    sum as spark_sum,
    to_json,
    to_timestamp,
    struct,
    window,
)

from streaming.schemas import TRANSACTION_SCHEMA, VALID_STATUSES


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("real-time-transaction-streaming")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "12"))
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .option("subscribe", os.getenv("KAFKA_TOPIC", "transactions.v1"))
        .option("startingOffsets", os.getenv("STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(kafka_df: DataFrame) -> DataFrame:
    raw = kafka_df.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_payload"),
    )

    parsed = raw.withColumn("event", from_json(col("raw_payload"), TRANSACTION_SCHEMA)).select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "raw_payload",
        "event.*",
    )

    return (
        parsed.withColumn("event_ts", to_timestamp("event_time"))
        .withColumn("ingested_ts", to_timestamp("ingested_at"))
        .withColumn(
            "error_reason",
            concat_ws(
                ";",
                expr("case when event_id is null or event_id = '' then 'missing_event_id' end"),
                expr("case when account_id is null or account_id = '' then 'missing_account_id' end"),
                expr("case when merchant_id is null or merchant_id = '' then 'missing_merchant_id' end"),
                expr("case when event_ts is null then 'invalid_event_time' end"),
                expr("case when amount is null or amount <= 0 then 'invalid_amount' end"),
                expr("case when currency is null or currency = '' then 'missing_currency' end"),
                expr("case when status not in ('approved','declined','reversed') then 'invalid_status' end"),
            ),
        )
    )


def valid_events(parsed: DataFrame) -> DataFrame:
    return parsed.filter(col("error_reason") == "").drop("error_reason").withWatermark("event_ts", "10 minutes").dropDuplicates(["event_id"])


def invalid_events(parsed: DataFrame) -> DataFrame:
    return parsed.filter(col("error_reason") != "")


def write_postgres(batch_df: DataFrame, table: str, columns: list[str], conflict_sql: str) -> None:
    if batch_df.rdd.isEmpty():
        return

    url = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/streaming_analytics")
    props = {
        "user": os.getenv("POSTGRES_USER", "streaming"),
        "password": os.getenv("POSTGRES_PASSWORD", "streaming"),
        "driver": "org.postgresql.Driver",
    }

    temp_table = f"{table}_staging"
    selected = batch_df.select(*columns)
    selected.write.jdbc(url=url, table=temp_table, mode="overwrite", properties=props)

    quoted_columns = ", ".join(columns)
    insert_columns = ", ".join([f'"{c}"' for c in columns])
    jdbc = selected.sparkSession._sc._gateway.jvm.java.sql.DriverManager.getConnection(url, props["user"], props["password"])
    statement = jdbc.createStatement()
    try:
        statement.execute(f"""
            insert into {table} ({insert_columns})
            select {quoted_columns} from {temp_table}
            {conflict_sql}
        """)
        statement.execute(f"drop table if exists {temp_table}")
    finally:
        statement.close()
        jdbc.close()


def write_processed(batch_df: DataFrame, batch_id: int) -> None:
    enriched = batch_df.withColumn("processed_at", current_timestamp()).withColumnRenamed("event_ts", "event_time").withColumnRenamed("ingested_ts", "ingested_at")
    columns = [
        "event_id",
        "event_type",
        "event_time",
        "ingested_at",
        "account_id",
        "merchant_id",
        "amount",
        "currency",
        "country",
        "channel",
        "status",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "processed_at",
    ]
    write_postgres(enriched, "processed_transactions", columns, "on conflict (event_id) do nothing")


def write_windows(batch_df: DataFrame, batch_id: int) -> None:
    enriched = (
        batch_df.withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn("processed_at", current_timestamp())
        .drop("window")
    )
    columns = [
        "window_start",
        "window_end",
        "country",
        "status",
        "transaction_count",
        "total_amount",
        "average_amount",
        "max_event_time",
        "processed_at",
    ]
    write_postgres(
        enriched,
        "transaction_windows",
        columns,
        """
        on conflict (window_start, window_end, country, status)
        do update set
            transaction_count = excluded.transaction_count,
            total_amount = excluded.total_amount,
            average_amount = excluded.average_amount,
            max_event_time = excluded.max_event_time,
            processed_at = excluded.processed_at
        """,
    )


def write_errors(batch_df: DataFrame, batch_id: int) -> None:
    errors = batch_df.withColumn("created_at", current_timestamp())
    columns = ["raw_payload", "error_reason", "kafka_topic", "kafka_partition", "kafka_offset", "created_at"]
    write_postgres(errors, "processing_errors", columns, "")


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    parsed = parse_events(read_kafka(spark))
    good = valid_events(parsed)
    bad = invalid_events(parsed)

    processed_query = (
        good.writeStream.foreachBatch(write_processed)
        .option("checkpointLocation", f"{os.getenv('CHECKPOINT_LOCATION', '/tmp/spark-checkpoints/transactions')}/processed")
        .trigger(processingTime=os.getenv("TRIGGER_INTERVAL", "10 seconds"))
        .queryName("processed_transactions")
        .start()
    )

    windows = good.groupBy(window(col("event_ts"), "5 minutes"), col("country"), col("status")).agg(
        count("*").alias("transaction_count"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount"),
        spark_max("event_ts").alias("max_event_time"),
    )

    window_query = (
        windows.writeStream.foreachBatch(write_windows)
        .outputMode("update")
        .option("checkpointLocation", f"{os.getenv('CHECKPOINT_LOCATION', '/tmp/spark-checkpoints/transactions')}/windows")
        .trigger(processingTime=os.getenv("TRIGGER_INTERVAL", "10 seconds"))
        .queryName("transaction_windows")
        .start()
    )

    error_query = (
        bad.writeStream.foreachBatch(write_errors)
        .option("checkpointLocation", f"{os.getenv('CHECKPOINT_LOCATION', '/tmp/spark-checkpoints/transactions')}/errors")
        .trigger(processingTime=os.getenv("TRIGGER_INTERVAL", "10 seconds"))
        .queryName("processing_errors")
        .start()
    )

    spark.streams.awaitAnyTermination()
    processed_query.stop()
    window_query.stop()
    error_query.stop()


if __name__ == "__main__":
    main()
