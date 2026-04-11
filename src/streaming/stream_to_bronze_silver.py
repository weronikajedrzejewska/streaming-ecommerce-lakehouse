import json
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")

BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze/events")
SILVER_EVENTS_PATH = os.getenv("SILVER_EVENTS_PATH", "data/silver/events")
SILVER_METRICS_PATH = os.getenv("SILVER_METRICS_PATH", "data/silver/metrics")

BRONZE_CHECKPOINT = os.getenv("BRONZE_CHECKPOINT", "data/checkpoints/bronze_events")
SILVER_EVENTS_CHECKPOINT = os.getenv("SILVER_EVENTS_CHECKPOINT", "data/checkpoints/silver_events")
SILVER_METRICS_CHECKPOINT = os.getenv("SILVER_METRICS_CHECKPOINT", "data/checkpoints/silver_metrics")

WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "5 minutes")


def load_schema() -> T.StructType:
    schema_path = Path(__file__).resolve().parents[1] / "schemas" / "event_schema.json"
    with schema_path.open("r", encoding="utf-8") as f:
        schema_json = json.load(f)
    return T.StructType.fromJson(schema_json)


def log_batch(df, epoch_id: int, label: str) -> None:
    start = time.time()
    batch_count = df.count()
    duration = round(time.time() - start, 2)
    print(f"[{label}] epoch={epoch_id} rows={batch_count} count_time_seconds={duration}")


def main() -> None:
    schema = load_schema()

    spark = (
        SparkSession.builder
        .appName("streaming_ecommerce_bronze_silver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Bronze layer keeps raw payload and ingestion metadata (append-only).
    bronze_df = kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("timestamp").alias("kafka_ingest_ts"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
    )

    bronze_query = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .queryName("bronze_events")
        .start()
    )

    parsed_df = (
        bronze_df
        .select(
            F.from_json(F.col("raw_json"), schema).alias("e"),
            F.col("kafka_ingest_ts"),
        )
        .select("e.*", "kafka_ingest_ts")
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
    )

    # Silver layer enforces schema and removes malformed or business-invalid records.
    silver_clean_df = parsed_df.filter(
        F.col("event_id").isNotNull()
        & F.col("event_ts").isNotNull()
        & ~(
            F.col("event_type").isin("add_to_cart", "purchase")
            & F.col("payload.price").isNull()
        )
    )

    silver_events_query = (
        silver_clean_df.writeStream
        .foreachBatch(lambda df, eid: log_batch(df, eid, "silver_events"))
        .option("checkpointLocation", SILVER_EVENTS_CHECKPOINT)
        .queryName("silver_events_observability")
        .start()
    )

    # Stateful windowed aggregation with watermark for late/out-of-order handling.
    silver_metrics_df = (
        silver_clean_df
        .withWatermark("event_ts", WATERMARK_DELAY)
        .groupBy(F.window("event_ts", WINDOW_DURATION))
        .agg(
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("carts_count"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases_count"),
            F.count("*").alias("events_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "events_count",
            "carts_count",
            "purchases_count",
        )
    )

    silver_metrics_query = (
        silver_metrics_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_METRICS_PATH)
        .option("checkpointLocation", SILVER_METRICS_CHECKPOINT)
        .queryName("silver_metrics")
        .start()
    )

    bronze_query.awaitTermination()
    silver_events_query.awaitTermination()
    silver_metrics_query.awaitTermination()


if __name__ == "__main__":
    main()
