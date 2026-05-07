# Model Serving — Calling Databricks AI Endpoints

## Use Case

These examples are for developers integrating with **Databricks Mosaic AI Model Serving** — whether calling foundation models like DBRX, custom-trained models, or LangGraph agents deployed as endpoints. They demonstrate multiple authentication and SDK patterns so you can choose the approach that fits your stack.

## What It Helps Implement

- **Calling a model serving endpoint** using three different approaches:
  - OpenAI Python SDK (drop-in compatible with Databricks endpoints)
  - Databricks Python SDK
  - Direct REST API calls
- **DBRX inference** via the OpenAI-compatible Pay-Per-Token endpoint — useful for quick integration without deploying your own model
- **Extracting model metadata** from MLflow-tracked models registered in Unity Catalog
- A foundation for ISVs and app developers to add LLM or AI agent capabilities by routing through Databricks Model Serving

## When to Use This

- You want to call a Databricks-hosted LLM or custom model from an external application
- You are an ISV looking to embed AI capabilities backed by Databricks Model Serving
- You want to compare OpenAI SDK, Databricks SDK, and REST API patterns for the same endpoint call
- You need to introspect deployed model metadata (e.g., input/output schema, MLflow run info)

## Key Concepts

| Concept | Description |
|---------|-------------|
| Model Serving Endpoint | A hosted REST endpoint that serves an MLflow model or foundation model |
| OpenAI SDK compatibility | Databricks endpoints are OpenAI API-compatible — just swap `base_url` |
| Pay-Per-Token (PAYGO) | Foundation models like DBRX available without deploying your own endpoint |
| Unity Catalog model registration | Models registered in UC can be versioned and served with governance |

## Prerequisites

- A Databricks workspace with Model Serving enabled
- A deployed model serving endpoint (or access to a PAYGO foundation model endpoint)
- A Databricks personal access token or notebook context credentials
- `openai`, `mlflow`, and/or `databricks-sdk` Python packages
