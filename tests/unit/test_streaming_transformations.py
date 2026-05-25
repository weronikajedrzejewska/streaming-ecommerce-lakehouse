import pytest
from pyspark.errors import PySparkRuntimeError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.streaming.stream_to_bronze_silver import (
    build_bronze_df,
    build_parsed_df,
    build_silver_clean_df,
    build_silver_metrics_df,
    load_schema,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    try:
        spark_session = (
            SparkSession.builder.master("local[1]").appName("streaming-tests").getOrCreate()
        )
    except PySparkRuntimeError as exc:
        pytest.skip(f"Spark is unavailable in this environment: {exc}")
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()


def test_build_silver_clean_df_filters_invalid_commerce_events(spark: SparkSession) -> None:
    schema = load_schema()
    raw_events = [
        (
            "key-1",
            '{"event_id":"evt-1","event_type":"add_to_cart","event_timestamp":"2026-04-20T10:00:00+00:00","ingestion_timestamp":"2026-04-20T10:00:01+00:00","user_id":"user-1","session_id":"session-1","country":"PL","payload":{"product_id":"prod-1","category":"books","price":20.0,"currency":"EUR","quantity":1,"cart_value":20.0}}',
            "2026-04-20T10:00:02+00:00",
            "ecommerce_events",
            0,
            1,
        ),
        (
            "key-2",
            '{"event_id":"evt-2","event_type":"purchase","event_timestamp":"2026-04-20T10:01:00+00:00","ingestion_timestamp":"2026-04-20T10:01:01+00:00","user_id":"user-2","session_id":"session-2","country":"PL","payload":{"product_id":"prod-2","category":"books","price":null,"currency":"EUR","quantity":1,"cart_value":30.0}}',
            "2026-04-20T10:01:02+00:00",
            "ecommerce_events",
            0,
            2,
        ),
    ]
    kafka_df = spark.createDataFrame(
        raw_events,
        ["key", "value", "timestamp", "topic", "partition", "offset"],
    ).withColumn("timestamp", F.to_timestamp("timestamp"))

    bronze_df = build_bronze_df(kafka_df)
    parsed_df = build_parsed_df(bronze_df, schema)
    silver_df = build_silver_clean_df(parsed_df)

    assert silver_df.count() == 1
    assert silver_df.select("event_id").first()[0] == "evt-1"


def test_build_silver_metrics_df_aggregates_static_batch(spark: SparkSession) -> None:
    rows = [
        ("evt-1", "add_to_cart", "2026-04-20T10:00:00+00:00"),
        ("evt-2", "purchase", "2026-04-20T10:01:00+00:00"),
        ("evt-3", "page_view", "2026-04-20T10:02:00+00:00"),
    ]
    static_df = spark.createDataFrame(
        rows, ["event_id", "event_type", "event_timestamp"]
    ).withColumn("event_ts", F.to_timestamp("event_timestamp"))

    metrics_df = build_silver_metrics_df(static_df)
    result = metrics_df.collect()

    assert len(result) == 1
    assert result[0]["events_count"] == 3
    assert result[0]["carts_count"] == 1
    assert result[0]["purchases_count"] == 1
