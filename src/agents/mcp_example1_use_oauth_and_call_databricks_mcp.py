# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### How the Code Works
# MAGIC
# MAGIC The code connects to a Databricks Managed MCP server using **OAuth authentication** to execute a Python script. It leverages the `databricks.sdk` and `databricks_mcp` libraries to interact with the Databricks API and the MCP server.
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC ### Key Steps in the Code
# MAGIC
# MAGIC 1.  **Authentication & Initialization**: The code first retrieves the Databricks host URL and OAuth client credentials (ID and secret) from either environment variables or Databricks secrets. It uses these credentials to initialize a `WorkspaceClient` with **`oauth-m2m`** (machine-to-machine) authentication. This is a secure, token-based method for applications to access Databricks resources without a user's password.
# MAGIC 2.  **Client Setup**: A URL for the managed MCP server's AI endpoint (`/api/2.0/mcp/functions/system/ai`) is constructed, and a **`DatabricksMCPClient`** is created. This client simplifies communication with the MCP server by handling lower-level details like streaming and authentication.
# MAGIC 3.  **Tool Discovery**: It connects to the server and lists the available tools. The output, `Discovered tools: ['system__ai__python_exec']`, confirms that the server provides a built-in Python code interpreter.
# MAGIC 4.  **Tool Execution**: The code then calls the `system__ai__python_exec` tool with a simple Python command: `print('Hello from the managed server with OAuth!')`. This command is executed remotely on the Databricks managed server.
# MAGIC 5.  **Result Retrieval**: Finally, it retrieves and prints the output from the tool execution, which is the string `Hello from the managed server with OAuth!`.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install -U "mcp>=1.9" "databricks-mcp"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

#from databricks.sdk import WorkspaceClient

# Get the host and token from the notebook context
#DATABRICKS_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
#DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Initialize the WorkspaceClient using explicit host and token details. (when not using oauth)
#workspace_client = WorkspaceClient(
#    host=DATABRICKS_HOST,
#    token=DATABRICKS_TOKEN
#)

# COMMAND ----------

# DBTITLE 1,Call databricks mcp server using oauth
import asyncio
import logging
import os
from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient

# Set up logging for better visibility into the process
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# This code is configured to use a managed MCP server.
# It's recommended to set your Databricks OAuth credentials as environment
# variables to avoid hardcoding them.
#DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://your-databricks-host.cloud.databricks.com")
#DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "YOUR_CLIENT_ID")
#DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "YOUR_CLIENT_SECRET")

DATABRICKS_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
DATABRICKS_CLIENT_ID = dbutils.secrets.get(scope="prasad_kona", key="DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = dbutils.secrets.get(scope="prasad_kona", key="DATABRICKS_CLIENT_SECRET")

async def call_managed_mcp_server():
    """
    Connects to a managed Databricks MCP server and calls a tool.
    This uses the high-level `DatabricksMCPClient` with OAuth authentication.
    """
    logger.info("Initializing Databricks workspace client with OAuth...")
    # Initialize the WorkspaceClient using explicit OAuth credentials.
    workspace_client = WorkspaceClient(
        host=DATABRICKS_HOST,
        client_id=DATABRICKS_CLIENT_ID,
        client_secret=DATABRICKS_CLIENT_SECRET,
        auth_type="oauth-m2m",
    )
    
    # Construct the URL for the managed MCP server. This endpoint
    # exposes built-in AI tools like a Python code interpreter.
    managed_mcp_server_url = f"{workspace_client.config.host}/api/2.0/mcp/functions/system/ai"
    
    logger.info(f"Connecting to managed MCP server at URL: {managed_mcp_server_url}")
    
    try:
        # The `DatabricksMCPClient` abstracts away the lower-level streaming and
        # authentication details, using the configured workspace client.
        mcp_client = DatabricksMCPClient(
            server_url=managed_mcp_server_url,
            workspace_client=workspace_client
        )
        
        logger.info("Client connected. Discovering available tools...")
        tools = mcp_client.list_tools()
        print(f"Discovered tools: {[t.name for t in tools]}")

        logger.info("Calling the 'system__ai__python_exec' tool...")
        
        # Call the Python code interpreter tool with a simple print statement.
        result = mcp_client.call_tool(
            "system__ai__python_exec", {"code": "print('Hello from the managed server with OAuth!')"}
        )
        
        logger.info("Tool call completed.")
        print(f"Result content from tool call: {result.content}")
                
    except Exception as e:
        logger.error(f"An error occurred: {e}")


async def main():
    """
    Main entry point for the asynchronous script.
    """
    await call_managed_mcp_server()


if __name__ == "__main__":
    asyncio.run(main())


# COMMAND ----------

import asyncio
import logging
import os
from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient

# Set up logging for better visibility into the process
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# This code is configured to use a managed MCP server.
# It's recommended to set your Databricks OAuth credentials as environment
# variables to avoid hardcoding them.
#DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://your-databricks-host.cloud.databricks.com")
#DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "YOUR_CLIENT_ID")
#DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "YOUR_CLIENT_SECRET")

DATABRICKS_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
DATABRICKS_CLIENT_ID = dbutils.secrets.get(scope="prasad_kona", key="DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = dbutils.secrets.get(scope="prasad_kona", key="DATABRICKS_CLIENT_SECRET")

async def call_managed_mcp_server():
    """
    Connects to a managed Databricks MCP server and calls a tool.
    This uses the high-level `DatabricksMCPClient` with OAuth authentication.
    """
    logger.info("Initializing Databricks workspace client with OAuth...")
    # Initialize the WorkspaceClient using explicit OAuth credentials.
    workspace_client = WorkspaceClient(
        host=DATABRICKS_HOST,
        client_id=DATABRICKS_CLIENT_ID,
        client_secret=DATABRICKS_CLIENT_SECRET,
        auth_type="oauth-m2m",
    )
    
    # Construct the URL for the managed MCP server. This endpoint
    # exposes built-in AI tools like a Python code interpreter.
    managed_mcp_server_url = f"{workspace_client.config.host}/api/2.0/mcp/functions/system/ai"
    
    logger.info(f"Connecting to managed MCP server at URL: {managed_mcp_server_url}")
    
    try:
        # The `DatabricksMCPClient` abstracts away the lower-level streaming and
        # authentication details, using the configured workspace client.
        mcp_client = DatabricksMCPClient(
            server_url=managed_mcp_server_url,
            workspace_client=workspace_client
        )
        
        logger.info("Client connected. Discovering available tools...")
        tools = mcp_client.list_tools()
        print(f"Discovered tools: {[t.name for t in tools]}")

        logger.info("Calling the 'system__ai__python_exec' tool...")
        
        # Call the Python code interpreter tool with a simple print statement.
        result = mcp_client.call_tool(
            "system__ai__python_exec", {"code": "print('Hello from the managed server with OAuth!')"}
        )
        
        logger.info("Tool call completed.")
        print(f"Result content from tool call: {result.content}")
                
    except Exception as e:
        logger.error(f"An error occurred: {e}")

async def main():
    """
    Main entry point for the asynchronous script.
    """
    await call_managed_mcp_server()

if __name__ == "__main__":
    asyncio.run(main())