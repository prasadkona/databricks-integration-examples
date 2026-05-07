# Databricks Integration Examples

A collection of practical integration patterns for developers and ISV partners building on the Databricks platform. Each example focuses on a specific Databricks capability and demonstrates how to connect to it, query it, or extend it from outside the platform.

## Examples

### [Agents — MCP Integration with OAuth](src/agents/README.md)
Connect an external AI agent to a **Databricks Managed MCP server** using OAuth M2M authentication. Demonstrates tool discovery and remote execution via the Model Context Protocol — the foundation for wiring external agents (LangChain, LangGraph, etc.) into Databricks capabilities.

---

### [AI/BI — Genie Conversations & Dashboard Metadata](src/ai_bi/README.md)
Two patterns for programmatic interaction with **Databricks AI/BI**:
- Chat with a Genie Space via the Conversation REST API — embed natural language data querying into any application
- Extract and audit dashboard metadata across a workspace using the Databricks SDK

---

### [Model Serving — Calling Databricks AI Endpoints](src/model_serving/README.md)
Call **Databricks Mosaic AI Model Serving** endpoints using three patterns: OpenAI Python SDK, Databricks SDK, and REST API. Includes DBRX Pay-Per-Token inference and MLflow model metadata extraction. The go-to reference for integrating LLM or agent endpoints into external applications.

---

### [Unity Catalog — Lineage & Metadata](src/unity_catalog/README.md)
Two Unity Catalog integration patterns:
- **Bring Your Own Lineage (BYOL)**: Register external systems (Salesforce, Qlik, Fivetran, etc.) as lineage participants in UC to build end-to-end data lineage visible in Catalog Explorer
- **PK/FK Constraints**: Define and query primary key / foreign key constraints on Delta tables for data modeling and BI integrations

---

### [Vector Search — Semantic Search with Databricks](src/vectorsearch/README.md)
Full lifecycle example for **Databricks Vector Search** using the Python SDK — create an endpoint, build a Delta Sync index, trigger syncs, and run similarity queries. The foundation for building RAG pipelines or semantic search over enterprise data in Unity Catalog.

---

## Prerequisites

All examples run as Databricks notebooks. You will need:
- A Databricks workspace (AWS, Azure, or GCP)
- Unity Catalog enabled (for Model Serving, Vector Search, and UC examples)
- A personal access token or service principal credentials
- Feature-specific prerequisites are listed in each subfolder's README

## Repository Structure

```
src/
├── agents/           # MCP + OAuth M2M integration
├── ai_bi/            # Genie Conversation API + dashboard metadata
├── model_serving/    # Model Serving endpoint patterns
├── unity_catalog/    # BYOL lineage + PK/FK constraints
└── vectorsearch/     # Vector Search SDK lifecycle
```
