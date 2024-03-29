from databricks.sdk import WorkspaceClient

# Instantiate the WorkspaceClient
w = WorkspaceClient()

#### Example 1 : Get list of model serving endpoint for foundational models or external models

# List model serving endpoints
endpoints = w.serving_endpoints.list()

# Placeholder list to hold foundational model endpoints
foundational_endpoints = []
external_model_endpoints = []

for endpoint in endpoints:
    # Retrieve detailed information for each endpoint
    endpoint_details = endpoint #w.serving_endpoints.get(endpoint.name)
    
    # Check if 'config' exists and is not None, then access 'served_entities'
    if hasattr(endpoint_details, 'config') and endpoint_details.config is not None:
        served_entities = getattr(endpoint_details.config, 'served_entities', None)
        if served_entities:
            # Iterate through each served entity to check for foundational models
            for entity in served_entities:
                # Ensure entity is not None and has a 'foundation_model' attribute
                if entity and hasattr(entity, 'foundation_model') and entity.foundation_model:
                    if entity.foundation_model.name:
                        foundational_endpoints.append(endpoint)
                        break  # Break the loop if at least one foundational model is found in the endpoint
                # Ensure entity is not None and has a 'foundation_model' attribute
                if entity and hasattr(entity, 'external_model') and entity.external_model:
                    if entity.external_model.name:
                        external_model_endpoints.append(endpoint)
                        break  # Break the loop if at least one external model is found in the endpoint

# Print the names of the foundational model endpoints
print("+++++ Example 1 : Get list of model serving endpoint for foundational models or external models")

print('List of foundational models')
for endpoint in foundational_endpoints:
    print('---Foundational Model---')
    print(f'Endpoint Name: {endpoint.name}')
    print(f'Endpoint ID: {endpoint.id}')
    print(f'State: {endpoint.state.ready}')
    if endpoint.config.served_entities :
        print(f'foundational_model_name: {endpoint.config.served_entities[0].foundation_model.name}')
        print(f'foundational_model_display_name: {endpoint.config.served_entities[0].foundation_model.display_name}')
print('---End---List of foundational models')

# Print the names of the foundational model endpoints
print('List of external models')
for endpoint in external_model_endpoints:
    print(endpoint.name)
print('---End---List of external models')




#### Example 2 : Get details for a model serving endpoint based on name

# Replace 'your_endpoint_name' with the actual name of your model serving endpoint
endpoint_name = 'databricks-mpt-7b-instruct' #'your_endpoint_name'

# Retrieve details about the specified model serving endpoint
endpoint_details = w.serving_endpoints.get(endpoint_name)

# Extract the external model information
external_model_info = endpoint_details.config.served_entities[0].external_model
foundational_model_name = endpoint_details.config.served_entities[0].foundation_model.name
foundational_model_diaplay_name = endpoint_details.config.served_entities[0].foundation_model.display_name

# Print the extracted external model information
print("+++++ Example 2 : Get details for a model serving endpoint based on name ")

print(f'Endpoint Name: {endpoint_details.name}')
print(f'Endpoint ID: {endpoint_details.id}')
print(f'State: {endpoint_details.state.ready}')
print(f'foundational_model_name: {endpoint_details.config.served_entities[0].foundation_model.name}')
print(f'foundational_model_display_name: {endpoint_details.config.served_entities[0].foundation_model.display_name}')

print("+++++++++++++++++++++++++++++")
