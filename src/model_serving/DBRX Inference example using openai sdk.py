# Databricks notebook source
# DBTITLE 1,OpenAI Python Environment Setup
# MAGIC %pip install openai --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Databricks OpenAI Client Configuration
# Next we will configure the OpenAI SDK with Databricks Access Token and our base URL
# To get your access token, go to User Settings --> Developer --> Access Tokens, and create one!

import openai
from openai import OpenAI

client = OpenAI(
    api_key="<replace this with your own access token!>",
    base_url="https://<databricks_workspace_url>/serving-endpoints"
)

# Check out the OpenAI Python SDK at https://platform.openai.com/docs/api-reference/chat/create 

# COMMAND ----------

# DBTITLE 1,Inference Chat with DBRX Model
import json

# Now let's invoke inference against the PAYGO (Pay Per Token) endpoint
response = client.chat.completions.create(
    model="databricks-dbrx-chat",
    messages=[
      {
        "role": "system", 
        "content": "You are a helpful assistant."},
      {
        "role": "user",
        "content": "What is a mixture of experts model?"
      }
    ]
)

json_output = json.dumps(json.loads(response.json()), indent=4)
print(json_output)

# COMMAND ----------

# DBTITLE 1,Provisioned Throughput Inference Endpoint Interaction
# Now let's invoke inference against the PT (Provisioned Throughput) endpoint
response = client.chat.completions.create(
    model="dbrx-chat-provisioned-throughput",
    messages=[
      {
        "role": "system", 
        "content": "You are a helpful assistant."},
      {
        "role": "user",
        "content": "What is a mixture of experts model?"
      }
    ]
)

json_output = json.dumps(json.loads(response.json()), indent=4)
print(json_output)

# COMMAND ----------


