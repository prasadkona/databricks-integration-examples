# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Extracting the metadata for AI+BI dashboards on a Databricks workspace
# MAGIC
# MAGIC This example uses the databricks-cli package to get all the details about dashboards deployed on a workspace.
# MAGIC
# MAGIC 1. makes a request to get the list of dashboards,
# MAGIC 2. iterates over each dashboard to get its details, permissions, and published details,
# MAGIC 3. handles exceptions for missing published details
# MAGIC 4. It writes dashboard details into a Databricks Delta table
# MAGIC 5. Finally it queries the table and displays the first 50 rows.

# COMMAND ----------

# Install the databricks-cli package
%pip install databricks-cli

from databricks_cli.sdk import ApiClient
import pandas as pd

# Get the token stored using dbutils
token = dbutils.secrets.get(scope="my_scope", key="my_personal_access_token")

# Initialize the API client with the host and token
host = "https://<my_databricks_host_url>"
api_client = ApiClient(host=host, token=token)

# COMMAND ----------

# DBTITLE 1,Get metadata for dashboards - v1 (first 20 only)
# Simple example that gets list of dashboards, permissions, details, published_details. 
# Refer to the next cell for V2 that persists all details extracted into a table and also handles exceptions

# Define the endpoint for listing dashboards
endpoint = "/lakeview/dashboards"

# Make the request to get the list of dashboards
response = api_client.perform_query("GET", endpoint)

# Extract the list of dashboards from the response
dashboards = response.get('dashboards', [])

# Initialize a list to store dashboard details
dashboard_details = []

# Iterate over each dashboard to get dashboard details
for dashboard in dashboards:
    dashboard_id = dashboard.get('dashboard_id')
    
    # Get permissions for the dashboard
    permissions_endpoint = f"/permissions/dashboards/{dashboard_id}"
    permissions_response = api_client.perform_query("GET", permissions_endpoint)
    dashboard['permissions'] = permissions_response
    
    # Get dashboard details
    dashboard_details_endpoint = f"/lakeview/dashboards/{dashboard_id}"
    dashboard_details_response = api_client.perform_query("GET", dashboard_details_endpoint)
    dashboard['details'] = dashboard_details_response
    
    # Get dashboard published_details
    dashboard_published_details_endpoint = f"/lakeview/dashboards/{dashboard_id}/published"
    try:
        dashboard_published_details_response = api_client.perform_query("GET", dashboard_published_details_endpoint)
    except Exception as e:
        if "404" in str(e):
                dashboard_published_details_response = {'error': str(e), 'details': {"error_code": "NOT_FOUND", "message": f"Unable to find published dashboard [dashboardId={dashboard_id}]"}}
        else:
            dashboard_published_details_response = str(e)
    dashboard['published_details'] = dashboard_published_details_response

    dashboard_details.append(dashboard)

# Create a DataFrame from the dashboard details
df_dashboards = pd.DataFrame(dashboard_details)

# Display the DataFrame
display(df_dashboards)


# COMMAND ----------

#Initialize the table . Drop table if exists
table_name = "main.default.dashboard_metadata"
spark.sql(f"drop table if exists {table_name}")

# COMMAND ----------

# DBTITLE 1,Get metadata for dashboards v2
from pyspark.sql.types import (
    StructType, StructField, StringType, MapType, ArrayType,DateType, BooleanType
)

# Define the schema for the DataFrame
schema = StructType([
    StructField("metadata_sync_id", StringType(), True),
    StructField("dashboard_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("create_time", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("lifecycle_state", StringType(), True),
    StructField("published", BooleanType(), True),
    StructField("permissions", MapType(StringType(), StringType()), True),
    StructField("details", MapType(StringType(), StringType()), True),
    StructField("published_details", MapType(StringType(), StringType()), True)
])

# Define the endpoint for listing dashboards
endpoint = "/lakeview/dashboards?page_size=200"

# Initialize a list to store all dashboard details
all_dashboard_details = []

# Initialize a sync id to track the sync status
metadata_sync_id = "20250219-1"

#Initialize the table name
table_name = "main.default.dashboard_metadata"

# Function to get dashboard details
def get_dashboard_details(endpoint):
    # Make the request to get the list of dashboards
    response = api_client.perform_query("GET", endpoint)

    # Extract the list of dashboards from the response
    dashboards = response.get('dashboards', [])
    
    # Iterate over each dashboard to get dashboard details
    for dashboard in dashboards:
        dashboard_id = dashboard.get('dashboard_id')
        
        # Get permissions for the dashboard
        permissions_endpoint = f"/permissions/dashboards/{dashboard_id}"
        try:
            permissions_response = api_client.perform_query("GET", permissions_endpoint)
        except Exception as e:
            permissions_response = {'error': str(e), 'details': {}}
        dashboard['permissions'] = permissions_response
        
        # Get dashboard details
        dashboard_details_endpoint = f"/lakeview/dashboards/{dashboard_id}"
        try:
            dashboard_details_response = api_client.perform_query("GET", dashboard_details_endpoint)
        except Exception as e:
            dashboard_details_response =  {'error': str(e), 'details': {}}
        dashboard['details'] = dashboard_details_response

        # Get dashboard published_details
        dashboard_published_details_endpoint = f"/lakeview/dashboards/{dashboard_id}/published"
        try:
            dashboard_published_details_response = api_client.perform_query("GET", dashboard_published_details_endpoint)
            dashboard['published'] =True
        except Exception as e:
            if "404" in str(e):
                dashboard_published_details_response = {'error': str(e), 'details': {"error_code": "NOT_FOUND", "message": f"Unable to find published dashboard [dashboardId={dashboard_id}]"}}
                dashboard['published'] = False
            else:
                dashboard_published_details_response = {'error': str(e), 'details': {}}
                dashboard['published'] = False

        dashboard['published_details'] = dashboard_published_details_response

        # set a sync id. This enables writing to the table and querying based on sync_id
        dashboard['metadata_sync_id'] = metadata_sync_id

        all_dashboard_details.append(dashboard)

    # Append to table
    df_all_dashboards_spark = spark.createDataFrame(all_dashboard_details, schema=schema)
    df_all_dashboards_spark.write.mode("append").saveAsTable(table_name)

    next_page_token = response.get('next_page_token')
    return next_page_token

# Initial call to get dashboard details
next_page_token = get_dashboard_details(endpoint)

# Initialize a counter for the number of next_page_tokens processed
token_counter = 0

# Call the API again if next_page_token is not null and counter is less than 2
while next_page_token and token_counter < 2000:
    next_page_endpoint = f"{endpoint}&page_token={next_page_token}"
    print("Processing: " + next_page_endpoint)
    next_page_token = get_dashboard_details(next_page_endpoint)
    token_counter += 1

# Display the DataFrame
#display(spark.createDataFrame(all_dashboard_details, schema=schema).limit(50))

# Display data from the table
spark.sql(f"select * from {table_name} limit 50").display()