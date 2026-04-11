# Streaming E-commerce Lakehouse

Mini streaming data engineering project for e-commerce events using Kafka-compatible ingestion and Spark Structured Streaming.

## Goal

Build a near-real-time pipeline that handles:
- streaming ingestion
- late and out-of-order events
- checkpoint-based recovery
- lakehouse-style Bronze and Silver layers

## Architecture

`Producer -> Redpanda (Kafka API) -> Spark Structured Streaming -> Bronze (Parquet) -> Silver (Parquet)`

## Tech Stack

- Python
- Redpanda (Kafka-compatible broker)
- PySpark Structured Streaming
- Parquet
- Local filesystem (MVP)

## Project Structure

```text
src/producer/kafka_producer.py
src/streaming/stream_to_bronze_silver.py
src/schemas/event_schema.json
docker/docker-compose.yml
sql/silver_metrics.sql
```

## Data Layers

- Bronze: raw, append-only events with ingestion timestamp
- Silver: cleaned, typed, filtered events and stateful windowed metrics

## Design Decisions

- Micro-batch streaming instead of continuous processing for simplicity and stability.
- Parquet instead of Delta to keep setup lightweight and dependency-free for MVP.
- At-least-once ingestion with idempotent-style transformations in Silver.

## Real-World Streaming Scenarios

- Late-arriving events are simulated in the producer.
- Out-of-order event timestamps are included by design.
- Watermark is used to bound state and handle delayed events.
- Checkpointing enables recovery after failure/restart.

## Quick Start

1. Start Redpanda:

```bash
docker compose -f docker/docker-compose.yml up -d
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run producer:

```bash
python src/producer/kafka_producer.py
```

4. Run streaming job:

```bash
python src/streaming/stream_to_bronze_silver.py
```

## Failure Recovery Check

1. Start streaming job and let data flow for 1-2 minutes.
2. Stop the streaming process.
3. Start it again with the same checkpoint location.
4. Confirm job resumes correctly and keeps processing new events.

## Next Steps

- Add dashboard for Silver metrics.
- Add data quality assertions for critical columns.
- Add cloud deployment variant (Kinesis/MSK + S3 + Glue/EMR).
