# Agents — MCP Integration with OAuth

## Use Case

This example is for ISV partners and developers who need to call a **Databricks Managed MCP (Model Context Protocol) server** from an external application or agent, using secure machine-to-machine (OAuth M2M) authentication — without hardcoding tokens or relying on user credentials.

## What It Helps Implement

- **Connecting to Databricks Managed MCP** via OAuth M2M authentication using the Databricks Python SDK
- **Discovering available tools** exposed by the MCP server (e.g., Python code execution, SQL, workspace APIs)
- **Invoking MCP tools remotely** — executing code or actions on Databricks from an external agent or automation pipeline
- A pattern for securely wiring external AI agents or orchestration frameworks (LangChain, LangGraph, etc.) into Databricks capabilities via MCP

## When to Use This

- You are building an AI agent outside of Databricks that needs to execute tasks on Databricks (e.g., run Python, query data)
- You want your agent to use Databricks as a tool provider via the MCP protocol
- You need a repeatable, credential-safe authentication pattern for service-to-service integrations

## Key Concepts

| Concept | Description |
|---------|-------------|
| OAuth M2M | Machine-to-machine auth using client ID + secret, no user login required |
| Databricks Managed MCP | A hosted MCP endpoint at `/api/2.0/mcp/functions/...` that exposes Databricks tools |
| Tool Discovery | MCP clients can list available tools before calling them |
| `DatabricksMCPClient` | SDK client that wraps MCP protocol communication with Databricks |

## Prerequisites

- A Databricks workspace with MCP enabled
- An OAuth M2M service principal with client ID and secret (stored in Databricks Secrets)
- `databricks-sdk` and `databricks-mcp` Python packages
