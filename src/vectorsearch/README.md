# Vector Search — Semantic Search with Databricks

## Use Case

This example is for developers building **RAG (Retrieval-Augmented Generation) pipelines** or **semantic search** applications on Databricks. It walks through the full lifecycle of a Vector Search index — from creating and syncing it, to querying it — using the Databricks Vector Search Python SDK.

## What It Helps Implement

- **Creating a Vector Search endpoint and index** backed by a Delta table in Unity Catalog
- **Keeping the index in sync** with the source Delta table as new data arrives
- **Querying the index** for semantically similar results using embedding-based search
- A complete, working pattern for the Vector Search SDK as an alternative to REST API calls
- Extended from the official Databricks documentation notebook with additional patterns for **programmatic table updates, sync triggering, and index verification**

## When to Use This

- You are building a RAG application and need a managed vector store on Databricks
- You want to use Delta tables as the source of truth for your embeddings (Delta Sync Index)
- You need a reproducible, SDK-based setup pattern for Vector Search rather than UI-driven configuration
- You are integrating an LLM application that needs similarity search over enterprise data stored in Unity Catalog

## Key Concepts

| Concept | Description |
|---------|-------------|
| Vector Search Endpoint | A managed compute resource that hosts one or more vector indexes |
| Delta Sync Index | An index that automatically syncs embeddings from a source Delta table |
| Embedding Model | A model serving endpoint that converts text to vectors (e.g., `databricks-gte-large-en`) |
| `VectorSearchClient` | Python SDK client for managing endpoints, indexes, and queries |
| Similarity Query | Search for records semantically closest to a given query string |

## Prerequisites

- A Databricks workspace with Vector Search enabled
- A Model Serving endpoint for an embedding model (e.g., `databricks-gte-large-en`)
- Unity Catalog with appropriate privileges to create tables and indexes
- `databricks-vectorsearch` Python package

## References

- [Vector Search Documentation](https://docs.databricks.com/aws/en/generative-ai/create-query-vector-search)
- [Vector Search Python SDK Example (official)](https://docs.databricks.com/aws/en/notebooks/source/generative-ai/vector-search-python-sdk-example.html)
