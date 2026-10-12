# Data Fabric Layer

This folder captures the cross-platform governance layer for the project.

## Where Data Fabric Is Used

- `catalog.yml` records ownership, domains, critical data assets, and lineage.
- `quality_contracts.yml` defines the data quality rules enforced by the Spark job.
- Kafka, Spark, PostgreSQL, Databricks, and Snowflake are represented as connected platform nodes instead of isolated tools.

In a production estate, these definitions would be loaded into a catalog such as Microsoft Purview, Unity Catalog, Collibra, or OpenMetadata.
