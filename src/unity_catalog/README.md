# Unity Catalog — Lineage & Metadata

## Use Case

These examples are for data engineers, platform teams, and ISV partners who need to extend **Databricks Unity Catalog** with external data context — either by bringing in lineage from outside Databricks, or by querying and enforcing data constraints like primary and foreign keys.

## What It Helps Implement

### Bring Your Own Lineage (BYOL)
- **Registering external systems** (e.g., Salesforce, MySQL, Qlik, Fivetran) as lineage participants in Unity Catalog
- **Creating end-to-end lineage** — connecting upstream source systems through ETL jobs into Databricks tables, visible in the Catalog Explorer
- An **idempotent lineage update pattern** — safe to run repeatedly; creates on first run, updates timestamps on subsequent runs
- Particularly useful for ISVs building ingestion or ETL products that want their pipelines to show up in Databricks data lineage

### Primary Key / Foreign Key Constraints
- **Defining PK/FK constraints** on Delta tables in Unity Catalog using SQL DDL
- **Querying constraint metadata** programmatically via the Databricks Python SDK
- Useful for data modeling, data quality enforcement, and BI tool integrations that rely on relational metadata

## When to Use This

- You are an ISV with an ingestion or ETL product and want your pipelines to appear in Databricks Unity Catalog lineage
- You need to track where data originated (e.g., Salesforce → Qlik → Databricks table) in a centralized lineage view
- You want to define and query relational constraints on Delta tables for downstream tooling (e.g., BI, data quality)

## Key Concepts

| Concept | Description |
|---------|-------------|
| BYOL (Bring Your Own Lineage) | UC API for registering external metadata and lineage not automatically captured |
| External Metadata API | REST API to represent external systems (tables, jobs) in UC |
| External Lineage API | REST API to establish lineage edges between external objects and UC tables |
| PK/FK Constraints | Informational constraints on Delta tables, stored in UC metadata |

## Prerequisites

- A Databricks workspace with Unity Catalog enabled
- Appropriate privileges to create/modify catalog objects
- For BYOL: access to the `ExternalMetadata` and `ExternalLineage` REST APIs
- `databricks-sdk` Python package

## References

- [Bring Your Own Lineage Docs](https://docs.databricks.com/aws/en/data-governance/unity-catalog/external-lineage)
- [ExternalMetadata API](https://docs.databricks.com/api/workspace/externalmetadata)
- [ExternalLineage API](https://docs.databricks.com/api/workspace/externallineage)
- [Table Constraints](https://docs.databricks.com/en/tables/constraints.html)
