# Snowflake Extension

This folder shows where Snowflake is used as the analytical warehouse layer.

## Where Snowflake Is Used

- Raw event landing with Snowpipe Streaming or a Kafka connector
- Curated transaction tables for BI and operational reporting
- Streams and tasks for incremental downstream transformations
- Query acceleration through clustering on event time and business dimensions

The local demo uses PostgreSQL because it is lightweight in Docker. In production, the same curated outputs can be written to Snowflake for larger analytical workloads and governed sharing.
