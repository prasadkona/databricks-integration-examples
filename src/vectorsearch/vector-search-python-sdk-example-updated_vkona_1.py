# Databricks notebook source
# MAGIC %md
# MAGIC This notebook 
# MAGIC - Is based on the notebook from  Databricks documentation https://docs.databricks.com/aws/en/generative-ai/create-query-vector-search?language=Python%C2%A0SDK
# MAGIC
# MAGIC - Direct link to original notebook https://docs.databricks.com/aws/en/notebooks/source/generative-ai/vector-search-python-sdk-example.html
# MAGIC
# MAGIC Some of the key updates to the original notebook
# MAGIC - Using python sdk to update the table, run a sync and verify that the index was updated

# COMMAND ----------

# MAGIC %md # Vector Search Python SDK example usage
# MAGIC
# MAGIC This notebook shows how to use the Vector Search Python SDK, which provides a `VectorSearchClient` as a primary API for working with Vector Search.
# MAGIC
# MAGIC Alternatively, you can call the REST API directly.
# MAGIC
# MAGIC ## Requirements
# MAGIC
# MAGIC This notebook assumes that a Model Serving endpoint named `databricks-gte-large-en` exists. To create that endpoint, see the notebook "Call a GTE embeddings model using Mosaic AI Model Serving" ([AWS](https://docs.databricks.com/en/generative-ai/create-query-vector-search.html#embedding-model-notebooks) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/create-query-vector-search#embedding-model-notebooks) | [GCP](https://docs.databricks.com/gcp/en/generative-ai/create-query-vector-search.html#embedding-model-notebooks)).

# COMMAND ----------

# MAGIC %pip install --upgrade --force-reinstall databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# COMMAND ----------

help(VectorSearchClient)

# COMMAND ----------

# MAGIC %md ## Load toy dataset into source Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC The following creates the source Delta table.

# COMMAND ----------


# Specify the catalog and schema to use. You must have USE_CATALOG privilege on the catalog and USE_SCHEMA and CREATE_TABLE privileges on the schema.
# Change the catalog and schema here if necessary.

catalog_name = "main"
schema_name = "default"


# COMMAND ----------

source_table_name = "en_wiki"
source_table_fullname = f"{catalog_name}.{schema_name}.{source_table_name}"

# COMMAND ----------

# Uncomment if you want to start from scratch.

# spark.sql(f"DROP TABLE {source_table_fullname}")

# COMMAND ----------

source_df = spark.read.parquet("/databricks-datasets/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet").limit(10)
display(source_df)

# COMMAND ----------

source_df.write.format("delta").option("delta.enableChangeDataFeed", "true").saveAsTable(source_table_fullname)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {source_table_fullname}"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create vector search endpoint

# COMMAND ----------

vector_search_endpoint_name = "vector-search-demo-endpoint"

# COMMAND ----------

try:
    vsc.create_endpoint(
        name=vector_search_endpoint_name,
        endpoint_type="STANDARD" # or "STORAGE_OPTIMIZED"
    )
except Exception as e:
    if "ALREADY_EXISTS" in str(e):
        print("Endpoint already exists, continuing...")
    else:
        raise e

# COMMAND ----------

endpoint = vsc.get_endpoint(
  name=vector_search_endpoint_name)
endpoint

# COMMAND ----------

# MAGIC %md ## Create vector index

# COMMAND ----------

# Vector index
vs_index = "en_wiki_index"
vs_index_fullname = f"{catalog_name}.{schema_name}.{vs_index}"

embedding_model_endpoint = "databricks-gte-large-en"

# COMMAND ----------

index = vsc.create_delta_sync_index(
  endpoint_name=vector_search_endpoint_name,
  source_table_name=source_table_fullname,
  index_name=vs_index_fullname,
  pipeline_type='TRIGGERED',
  primary_key="id",
  embedding_source_column="text",
  embedding_model_endpoint_name=embedding_model_endpoint
)
index.describe()

# COMMAND ----------

# MAGIC %md ## Get a vector index  
# MAGIC
# MAGIC Use `get_index()` to retrieve the vector index object using the vector index name. You can also use `describe()` on the index object to see a summary of the index's configuration information.

# COMMAND ----------



index = vsc.get_index(endpoint_name=vector_search_endpoint_name, index_name=vs_index_fullname)

index.describe()

# COMMAND ----------

# Wait for index to come online. Expect this command to take several minutes.
import time
while not index.describe().get('status').get('detailed_state').startswith('ONLINE'):
  print("Waiting for index to be ONLINE...")
  time.sleep(5)
print("Index is ONLINE")
index.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC # Update Index and trigger vector search index sync
# MAGIC
# MAGIC
# MAGIC A function to Update the Delta table and triggers a vector index sync in triggered mode.

# COMMAND ----------

import time
from databricks.vector_search.client import VectorSearchClient

def update_table_and_trigger_sync(spark, update_query, endpoint_name, index_name):
    """
    Updates the Delta table and triggers a vector index sync in triggered mode.
    Ensures that no concurrent syncs are running by checking status with describe().
    Waits for the index to be ONLINE and for the new sync to complete.
    Handles errors gracefully.
    """
    try:
        # Step 1: Update the Delta table
        spark.sql(update_query)
        print("Delta table updated successfully.")
    except Exception as e:
        print(f"Error updating Delta table: {str(e)}")
        return

    try:
        # Step 2: Initialize the Vector Search client and get the index
        client = VectorSearchClient()
        index = client.get_index(endpoint_name=endpoint_name, index_name=index_name)

        # Step 3: Wait for index to be ONLINE
        print("Checking if index is ONLINE...")
        while True:
            status = index.describe()
            detailed_state = (
                status.get('status', {}).get('detailed_state') or
                status.get('detailed_state')  # fallback if structure is different
            )
            print(f"Current detailed_state: {detailed_state}")
            if detailed_state and detailed_state.startswith('ONLINE'):
                print("Index is ONLINE.")
                break
            print("Waiting for index to be ONLINE...")
            time.sleep(5)
        print("Index is ONLINE. Full status:")
        print(index.describe())

        # Step 4: Check sync status and wait if a sync is running
        sync_status = status.get("sync_status") or status.get("status")
        if sync_status in ("RUNNING", "IN_PROGRESS"):
            print("A sync is currently running. Waiting for it to complete...")
            index.wait_until_ready(wait_for_updates=True)
            print("Previous sync completed. Ready for a new sync.")
        else:
            print("No sync currently running. Proceeding.")

        # Step 5: Trigger a new sync
        index.sync()
        print("Sync triggered. Waiting for sync to complete...")

        # Step 6: Wait for the new sync to finish
        index.wait_until_ready(wait_for_updates=True)
        print(f"Sync completed successfully for index: {index_name}")

    except Exception as e:
        print(f"Error during sync process for index {index_name}: {str(e)}")
        # Optional: Add logging, retries, or alerting here

# Example usage
update_query = f"UPDATE {source_table_fullname} SET text = 'new text' WHERE id = 10002"
endpoint_name = vector_search_endpoint_name
index_name = vs_index_fullname

update_table_and_trigger_sync(spark, update_query, endpoint_name, index_name)


# COMMAND ----------

# MAGIC %md ## Similarity search
# MAGIC
# MAGIC Query the Vector Index to find similar documents.

# COMMAND ----------

# Returns [col1, col2, ...]
# You can set this to any subset of the columns.
all_columns = spark.table(source_table_fullname).columns

results = index.similarity_search(
  query_text="Greek myths",
  columns=all_columns,
  num_results=2)

results

# COMMAND ----------

#Print the data from the vs index to verify that the data was updated

# Perform a scan
results = index.scan(num_results=10)

# Extract the values for keys 'id' and 'text' from results['data'], truncating 'text' to 100 characters
values = [
    {
        field['key']: (
            field['value']['string_value'][:100] if field['key'] == 'text' else field['value']
        ) if field['key'] == 'text' else (
            field['value']['number_value'] if field['key'] == 'id' else field['value']
        )
        for field in item['fields'] if field['key'] in ('id', 'text')
    }
    for item in results['data']
]

values

# COMMAND ----------

# Search with a filter. Note that the syntax depends on the endpoint type.

# Standard endpoint syntax
results = index.similarity_search(
  query_text="Greek myths",
  columns=all_columns,
  filters={"id NOT": ("13770", "88231")},
  num_results=2)

# Storage-optimized endpoint syntax
# results = index.similarity_search(
#   query_text="Greek myths",
#   columns=all_columns,
#   filters='id NOT IN ("13770", "88231")',
#   num_results=2)

results


# COMMAND ----------

# MAGIC %md ## Convert results to LangChain documents
# MAGIC
# MAGIC The first column retrieved is loaded into `page_content`, and the rest into metadata.

# COMMAND ----------

# You must have langchain installed on your cluster. The following command installs or upgrades langchain.
%pip install --upgrade langchain

# COMMAND ----------

from langchain.schema import Document
from typing import List

def convert_vector_search_to_documents(results) -> List[Document]:
  column_names = []
  for column in results["manifest"]["columns"]:
      column_names.append(column)

  langchain_docs = []
  for item in results["result"]["data_array"]:
      metadata = {}
      score = item[-1]
      # print(score)
      i = 1
      for field in item[1:-1]:
          # print(field + "--")
          metadata[column_names[i]["name"]] = field
          i = i + 1
      doc = Document(page_content=item[0], metadata=metadata)  # , 9)
      langchain_docs.append(doc)
  return langchain_docs

langchain_docs = convert_vector_search_to_documents(results)

langchain_docs

# COMMAND ----------

# MAGIC %md ## Delete vector index

# COMMAND ----------

vsc.delete_index(index_name=vs_index_fullname)