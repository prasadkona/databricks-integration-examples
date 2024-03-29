############################################
#
# The purpose of this example is to demonstrate how to call model serving endpoints
#
############################################
# Authors          : Prasad Kona
# Organization     : Databricks
# Version          : 1.0
# Last update date : May 26, 2023
############################################

import os
import requests

#setting the environment variables for conenction details (optional)
    #export DATABRICKS_HOST=https://myshard.cloud.databricks.com
    #       i.e DATABRICKS_WORKSPACE_HOST_URI
    #export DATABRICKS_TOKEN=MY_TOKEN

os.environ["DATABRICKS_TOKEN"] = "DATABRICKS_PERSONAL_ACCESS_TOKEN"
DATABRICKS_WORKSPACE_URL = "DATABRICKS_WORKSPACE_HOST_URI"
MLFLOW_MODEL_SERVING_ENDPOINT_NAME = "dbdemos_fsi_credit_decisioning_endpoint" # example endpoint name from your workspace


# Get the details for a model serving endpoint
#   API doc : https://docs.databricks.com/api-explorer/workspace/servingendpoints/get
#   API     : GET/api/2.0/serving-endpoints/{name}

def get_endpoint_info(endpoint_name):
    token = os.getenv('DATABRICKS_TOKEN')
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    url = f'{DATABRICKS_WORKSPACE_URL}/api/2.0/serving-endpoints/{MLFLOW_MODEL_SERVING_ENDPOINT_NAME}'
    response = requests.request(method='GET', headers=headers, url=url)
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    return response.json()['config']['served_models']

# Retrieve all model serving endpoints
#   API Doc : https://docs.databricks.com/api-explorer/workspace/servingendpoints/list
#   API     : GET /api/2.0/serving-endpoints

def get_all_endpoints():
    token = os.getenv('DATABRICKS_TOKEN')
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    url = f'{DATABRICKS_WORKSPACE_URL}/api/2.0/serving-endpoints'
    response = requests.request(method='GET', headers=headers, url=url)
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    
    return response.json()['endpoints']


#Below are additional API that could be useful 

# Retrieve details for a model and model version
#   API Doc : https://docs.databricks.com/api-explorer/workspace/modelregistry/getmodelversion
#   API     : GET /api/2.0/mlflow/model-versions/get
#   Notes   : Respose has run_id that you can use with the mlflow-run api below

# Retrieve details for a run
#   API Doc : https://docs.databricks.com/api-explorer/workspace/experiments/getrun
#   API     : GET /api/2.0/mlflow/runs/get



print("\n\nAll model serving endpoint :\n\n", get_all_endpoints())

print("\n\nDetails for model serving endpoint :",MLFLOW_MODEL_SERVING_ENDPOINT_NAME,'\n\n',
      get_endpoint_info(MLFLOW_MODEL_SERVING_ENDPOINT_NAME))

