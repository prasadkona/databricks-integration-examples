# Databricks notebook source
# MAGIC %pip install openai mlflow 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Three ways to call Databricks Mosaic AI model serving endpoints
# MAGIC * 1. Using OpenAI sdk to call model serving endpoint
# MAGIC * 2. Using Databricks sdk to call model serving endpoint
# MAGIC * 3. Using rest api to call model serving endpoint

# COMMAND ----------

catalog = "prasad_kona_isv"
schema = "demo"
model_name = "langgraph-tool-calling-agent"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# COMMAND ----------

# DBTITLE 1,Using OpenAI sdk to call model serving endpoint
from openai import OpenAI
import os


# In a Databricks notebook you can use this:
DATABRICKS_HOSTNAME = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
#DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
# Alternatively in a Databricks notebook you can use this:
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

serving_endpoint_name ="agents_prasad_kona_isv-demo-langgraph-tool-calling-agent"

client = OpenAI(
  api_key=DATABRICKS_TOKEN,
  base_url=f"https://{DATABRICKS_HOSTNAME}/serving-endpoints"
)

chat_completion = client.chat.completions.create(
  messages=[
  {
    "role": "system",
    "content": "You are an AI assistant"
  },
  {
    "role": "user",
    "content": "Tell me about Large Language Models in one sentence"
  }
  ],
  model= serving_endpoint_name,
  max_tokens=256
)

print(chat_completion.choices[0].message.content) if chat_completion and chat_completion.choices else print(chat_completion)



# COMMAND ----------

# DBTITLE 1,Using Databricks sdk to call model serving endpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import DataframeSplitInput

# In a Databricks notebook you can use this:
#databricks_hostname = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName()
#databricks_token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

serving_endpoint_name ="agents_prasad_kona_isv-demo-langgraph-tool-calling-agent"

#endpoint_url = f"https://{databricks_hostname}/serving-endpoints/{serving_endpoint_name}/invocations"


w = WorkspaceClient()

test_dialog = DataframeSplitInput(
    columns=["messages"],
    data=[
        {
            "messages": [
                {"role": "user", "content": "How does billing work on Databricks? Answer in one sentence"},
            ]
        }
    ],
)
answer = w.serving_endpoints.query(serving_endpoint_name, dataframe_split=test_dialog)
print(answer.predictions)

# COMMAND ----------

# DBTITLE 1,Using rest api to call model serving endpoint
import json
import requests

# In a Databricks notebook you can use this:
DATABRICKS_TOKEN=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
DATABRICKS_HOSTNAME = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
serving_endpoint_name ="agents_prasad_kona_isv-demo-langgraph-tool-calling-agent"

endpoint_url = f"https://{DATABRICKS_HOSTNAME}/serving-endpoints/{serving_endpoint_name}/invocations"

input_example = {
    "messages": [
        {
            "role": "user",
            "content": "How does billing work on Databricks? Answer in one sentence",
        }
    ]
}

def call_model_serving_endpoint(payload):
    url = endpoint_url
    headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}', 'Content-Type': 'application/json'}
    
    data_json = json.dumps(payload, allow_nan=True)
    response = requests.request(method='POST', headers=headers, url=url, data=data_json)
    return response.json()

response_data = call_model_serving_endpoint(input_example)
display(response_data)