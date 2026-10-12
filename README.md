# Real-Time Streaming & Event Processing Platform

A senior-level streaming analytics project that processes operational transaction events through Kafka, Spark Structured Streaming, and PostgreSQL. The local Docker stack is designed to demonstrate event design, partitioning, offsets, checkpointing, deduplication, late-arriving data, windowed aggregations, observability, and recovery behavior.

## Architecture

```text
Python Kafka Producer
        |
        v
Kafka topics
  - transactions.v1
  - transactions.invalid.v1
        |
        v
Spark Structured Streaming
  - schema validation
  - duplicate handling
  - watermarking for late events
  - windowed aggregations
  - PostgreSQL sink
        |
        v
PostgreSQL analytics tables
  - processed_transactions
  - transaction_windows
  - processing_errors
```

The platform is intentionally local-first. The same design can be lifted into Databricks, Snowflake, or a broader data fabric architecture; see [Platform Extensions](#platform-extensions).

## Technologies

- Apache Kafka for event transport
- Python producer for synthetic operational events
- Spark Structured Streaming for stateful processing
- PostgreSQL for serving near-real-time analytics
- Docker Compose for repeatable local infrastructure
- pytest for unit-level validation of schema and business rules

## Quick Start

Prerequisites:

- Docker Desktop
- Python 3.11+ for local tests

Start the platform:

```powershell
docker compose up --build
```

In another terminal, generate events:

```powershell
docker compose run --rm producer python -m producer.generate_events --events 5000 --rate-per-second 50
```

Check analytics tables:

```powershell
docker compose exec postgres psql -U streaming -d streaming_analytics -c "select * from transaction_windows order by window_start desc limit 10;"
docker compose exec postgres psql -U streaming -d streaming_analytics -c "select status, count(*) from processed_transactions group by status;"
docker compose exec postgres psql -U streaming -d streaming_analytics -c "select * from processing_errors order by created_at desc limit 10;"
```

Monitor consumer groups:

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --all-groups
```

## Event Schema

Topic: `transactions.v1`

Partition key: `account_id`

```json
{
  "event_id": "uuid",
  "event_type": "transaction_authorized",
  "event_time": "2026-10-12T10:15:30.000Z",
  "ingested_at": "2026-10-12T10:15:31.000Z",
  "account_id": "acct-10001",
  "merchant_id": "mrc-321",
  "amount": 42.95,
  "currency": "USD",
  "country": "US",
  "channel": "card_present",
  "status": "approved"
}
```

The producer occasionally emits duplicates, invalid records, and late events so the streaming job has real failure modes to handle.

## Topic Design

| Topic | Partitions | Retention | Purpose |
| --- | ---: | --- | --- |
| `transactions.v1` | 6 | 7 days | Main event stream keyed by account |
| `transactions.invalid.v1` | 3 | 14 days | Invalid source events for replay and diagnosis |

Partitioning by `account_id` keeps events for an account ordered within a partition. This is useful for operational analytics where account-level consistency matters. The trade-off is possible skew if a small number of accounts produce very high volume; in production, hot-account mitigation can use a composite key such as `account_id:event_type` or a deterministic shard suffix.

## Processing Semantics

Spark reads from Kafka with committed offsets tracked in its checkpoint directory. The job uses:

- explicit JSON schema parsing
- validation rules for required IDs, event timestamps, status, currency, and positive amounts
- `event_id` deduplication with watermarking
- event-time windows with `withWatermark`
- a `foreachBatch` sink to PostgreSQL using idempotent upserts

The local pipeline targets effectively-once results for PostgreSQL tables. Kafka-to-Spark is replayable from offsets, Spark state is recoverable from checkpoints, and PostgreSQL writes use primary keys plus `ON CONFLICT` upserts. This is not the same as a distributed transaction across Kafka, Spark, and PostgreSQL; if a batch partially commits and the driver fails, replayed batches must remain idempotent.

## Late Data and Deduplication

- Watermark: 10 minutes on `event_time`
- Aggregation windows: 5-minute tumbling windows
- Deduplication key: `event_id`
- Duplicate handling: repeated `event_id` values are dropped before aggregation
- Late handling: records older than the watermark may be ignored by stateful aggregations

This favors bounded state and predictable memory use. Increasing the watermark improves correctness for delayed events but increases Spark state size, checkpoint volume, and recovery time.

## Delivery Guarantees

| Layer | Guarantee | Notes |
| --- | --- | --- |
| Producer to Kafka | At-least-once | Producer retries are enabled. Duplicate event IDs are intentionally possible. |
| Kafka storage | Durable while retained | Local broker uses single-node durability for demo purposes. Production should use replication factor 3+. |
| Spark processing | Recoverable offsets and state | Checkpointing allows restart from the last successful micro-batch. |
| PostgreSQL sink | Effectively-once upserts | Idempotent keys protect against replayed micro-batches. |

## Latency vs Reliability

The streaming job uses a 10-second trigger by default. Lower triggers reduce latency but increase scheduling overhead and database write frequency. Larger triggers improve throughput and reduce sink pressure but make dashboards less fresh. For stricter reliability, use longer checkpoint retention, Kafka replication, PostgreSQL high availability, and dead-letter replay workflows.

## Observability

The project exposes these operational signals:

- Kafka consumer lag through `kafka-consumer-groups`
- Spark progress logs for input rows, batch duration, and watermark progress
- PostgreSQL freshness via `max(processed_at)` in serving tables
- invalid record counts in `processing_errors`
- processed volume by status and country

Example freshness check:

```sql
select now() - max(processed_at) as data_freshness_lag
from processed_transactions;
```

## Recovery Test

1. Start the stack with `docker compose up --build`.
2. Generate a steady stream of events.
3. Stop the Spark container:

   ```powershell
   docker compose stop spark-streaming
   ```

4. Continue producing events.
5. Restart Spark:

   ```powershell
   docker compose start spark-streaming
   ```

6. Verify that Spark resumes from checkpointed Kafka offsets and PostgreSQL tables do not double-count replayed events.

## Platform Extensions

### Databricks

On Databricks, the Spark job can be moved to a Delta Live Tables or Structured Streaming job. Replace PostgreSQL serving tables with Delta tables, enable Unity Catalog governance, and use Auto Loader or Kafka source authentication for managed ingestion. Checkpoints should live in cloud object storage with lifecycle policies.

### Snowflake

For Snowflake, Kafka events can land through Snowpipe Streaming or a Kafka connector into raw tables. Spark can still enrich and aggregate events, then write curated results to Snowflake. Use streams and tasks for downstream incremental models and define clustering on event time and account dimensions where query patterns justify it.

### Data Fabric

The data fabric layer is the governance and interoperability plane across Kafka, Spark, PostgreSQL, Databricks, and Snowflake. In a production version, this would include a schema registry, data catalog, lineage, quality contracts, access policies, and common observability standards across batch and streaming assets.

## Repository Layout

```text
.
├── docker-compose.yml
├── sql/
│   └── init.sql
├── src/
│   ├── producer/
│   │   └── generate_events.py
│   └── streaming/
│       ├── schemas.py
│       └── spark_job.py
├── tests/
│   └── test_validation.py
├── requirements.txt
└── README.md
```

## Run Tests

```powershell
pip install -r requirements.txt
pytest
```

## Limitations

- The Docker stack uses a single Kafka broker for local simplicity.
- PostgreSQL is a serving store here, not a high-volume OLAP engine.
- Exactly-once behavior across Kafka, Spark, and PostgreSQL is approximated through checkpointing and idempotent writes.
- No external schema registry is included locally, although the schema is centralized in code.
