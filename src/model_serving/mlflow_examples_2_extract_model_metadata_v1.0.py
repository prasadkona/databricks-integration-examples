############################################
#
# The purpose of the example is to demonstrate for a given mlflow model
# 1. How to extract the input and output schemas
# 2. How to extract the input example data associated with the mlflow model
#
############################################
# Authors          : Prasad Kona, Paul Ogilvie
# Organization     : Databricks
# Version          : 1.0
# Last update date : May 25, 2023
############################################

import os
import mlflow
import yaml
import tempfile
from mlflow.tracking import MlflowClient


#setting the environment variables required by mlflow client and mlflow cli
    #export MLFLOW_TRACKING_URI=databricks
    #export DATABRICKS_HOST=https://myshard.cloud.databricks.com
    #export DATABRICKS_TOKEN=MY_TOKEN

os.environ["MLFLOW_TRACKING_URI"] = "databricks"
os.environ["DATABRICKS_HOST"] = "https://databricks-url"
os.environ["DATABRICKS_TOKEN"] = "MY_DATABRICKS_PERSONAL_ACCESS_TOKEN"

# In this example we set the name of the model and the version
# You can get the model name and version for a mlflow model serving endpoint using Databricks rest apis'
#  1. Get list of all model serving endpoints
#     GET DATABRICKS_HOST/api/2.0/serving-endpoints
#  2. Get details for a model serving endpoint
#     GET DATABRICKS_HOST/api/2.0/serving-endpoints/<databricks_modelserving_endpoint_name>
# Model used in this demo is from https://www.dbdemos.ai/demo.html?demoName=lakehouse-retail-c360
# You can use any model that you have on the model registry
registered_model_name = "dbdemos_customer_churn"
version = 5

#Set up the MLflow tracking URI to connect to your Databricks workspace
mlflow.set_tracking_uri("databricks")

#Create an MLflow client: connects to Databricks workspace by using environment variables
client = MlflowClient()

#Example on how to Get a model by name
#model = client.get_registered_model("dbdemos_customer_churn")
#print(model)

#Download the mlflow model
model_uri = f'models:/{registered_model_name}/{version}'
download_dir = tempfile.gettempdir()
model_file_path = mlflow.artifacts.download_artifacts(f'{model_uri}/MLmodel', dst_path=download_dir)

#Load the model info yaml file
with open(model_file_path) as file:
  model_info = yaml.safe_load(file)

# Getting the input example file
#  saved_input_example_info => This contains the 'artifact_path' which is path+name of the input example present in the model
example_artifact_path = model_info['saved_input_example_info']['artifact_path']

#Download the input example file for the mlflow model
example_file_path = mlflow.artifacts.download_artifacts(f'{model_uri}/{example_artifact_path}', dst_path=download_dir)

print("\n\nModel Local Path - Download Dir : \n", download_dir)
print("\n\nModel Local Path - Model file: \n", model_file_path)
print("\n\nModel Local Path - input example  : \n", example_file_path)
print("\n\nModel Info : \n", model_info)

# load the mlflow model from the model file
the_model = mlflow.models.Model.load(model_file_path)

# Extract the input schema for the mlflow model
the_model_input_schema = the_model.get_input_schema()
print("\n\nModel Input Schema : \n", the_model_input_schema) 

# Extract the output schema for the mlflow model
the_model_output_schema = the_model.get_output_schema()
print("\n\nModel Output Schema : \n", the_model_output_schema) 

#Extract information on the saved_input_example
print("\n\nInformation on the saved_input_example : \n", the_model.saved_input_example_info)


#Extract the saved input example json for the mlflow model
saved_input_example_json = {"dataframe_split": mlflow.models.Model.load(download_dir).load_input_example(download_dir).to_dict(orient='split')}
print("\n\nSaved Input Example json : \n",saved_input_example_json)





# Alternative Method 2: Extract the input and output signatures for the model
#print("\n\nInformation on the signature (input and output) : \n", mlflow.models.signature.ModelSignature.from_dict(model_info['signature']))

# Alternative Method 2 :Extract information on the saved_input_example
#print("\n\nInformation on the saved_input_example : \n", model_info["saved_input_example_info"])
