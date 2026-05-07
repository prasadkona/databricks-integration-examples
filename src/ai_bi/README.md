# AI/BI — Genie Conversations & Dashboard Metadata

## Use Case

These examples are for developers and data platform teams who need to **programmatically interact with Databricks AI/BI** — either by having conversations with a Genie Space via API, or by extracting metadata about dashboards deployed on a workspace.

## What It Helps Implement

### Genie Conversation API
- **Chatting with your data via code** — send natural language questions to a Genie Space and receive SQL-backed answers programmatically
- **Multi-turn conversations** — send an initial question and chain follow-up questions in a single session
- A pattern for embedding Genie-powered Q&A into external applications, Slack bots, or internal tools

### Dashboard Metadata Extraction
- **Auditing AI/BI dashboards** across a workspace — list all dashboards, their owners, permissions, and published status
- **Persisting dashboard metadata** into a Delta table for governance, reporting, or change tracking
- Useful for workspace administrators or ISVs building observability tooling around Databricks Lakeview dashboards

## When to Use This

- You want to embed Databricks natural language data querying into an app or workflow outside the Databricks UI
- You need to inventory or audit dashboards across a workspace programmatically
- You are building integrations that need to discover what dashboards exist and who owns them

## Key Concepts

| Concept | Description |
|---------|-------------|
| Genie Space | A configured AI/BI environment with datasets, sample queries, and guidelines for NL-to-SQL |
| Genie Conversation API | REST API for starting and continuing multi-turn conversations with a Genie Space |
| Lakeview Dashboards | Databricks AI/BI dashboards (next-gen, separate from legacy dashboards) |
| Dashboard Metadata | Details including owner, permissions, published state, and dataset bindings |

## Prerequisites

- A Databricks workspace with AI/BI Genie enabled
- A Genie Space ID for conversation examples
- A Databricks personal access token or service principal credentials
- `databricks-sdk` or `databricks-cli` Python package (depending on the example)

## References

- [Genie Documentation](https://docs.databricks.com/aws/en/genie/)
- [Genie REST API](https://docs.databricks.com/api/workspace/genie)
- [Genie Python SDK](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/dashboards/genie.html)
