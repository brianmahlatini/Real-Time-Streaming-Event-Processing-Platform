# Databricks Extension

This folder shows how the local Spark Structured Streaming job maps to a managed Databricks implementation.

## Where Databricks Is Used

- Managed Spark runtime for the streaming processor
- Delta Lake bronze/silver/gold tables for raw, validated, and aggregated events
- Unity Catalog for governed table access
- Cloud object storage for checkpoints
- Databricks Workflows for production scheduling and restart policies

The local Docker version writes to PostgreSQL for easy demonstration. The Databricks version writes to Delta tables so analytics, lineage, time travel, and replay are managed through the lakehouse.
