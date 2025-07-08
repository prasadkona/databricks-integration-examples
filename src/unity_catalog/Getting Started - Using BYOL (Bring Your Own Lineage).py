# Databricks notebook source
# MAGIC %md
# MAGIC # Getting Started - Using BYOL (Bring Your Own Lineage)
# MAGIC
# MAGIC This notebooks walks you through using how to your own lineage entries using Databricks Unity Catalog's "Bring your own data lineage"
# MAGIC
# MAGIC
# MAGIC Unity Catalog automatically captures runtime data lineage across queries that are run on Databricks. However, you might have workloads that run outside of Databricks (for example, first mile ETL or last mile BI). Unity Catalog lets you add external lineage metadata to augment the Databricks data lineage it captures automatically, giving you an end-to-end lineage view in Unity Catalog. This is useful when you want to capture where data came from (for example, Salesforce or MySQL) before it was ingested into Unity Catalog or where data is being consumed outside of Unity Catalog (for example, Tableau or PowerBI).
# MAGIC
# MAGIC
# MAGIC Link to product doc
# MAGIC - https://docs.databricks.com/aws/en/data-governance/unity-catalog/external-lineage
# MAGIC
# MAGIC Link to API docs 
# MAGIC - https://docs.databricks.com/api/workspace/externalmetadata
# MAGIC - https://docs.databricks.com/api/workspace/externallineage
# MAGIC
# MAGIC
# MAGIC <br><br>
# MAGIC Author: Prasad Kona <br>
# MAGIC Last Update date: July 8, 2025

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction
# MAGIC This Python script automates the creation and maintenance of data lineage in Databricks Unity Catalog for an external ETL process. This leverages the Databricks REST api's.
# MAGIC
# MAGIC It is designed to be run every time an external job (like a Qlik task, Fivetran sync, or other ISV product pipeline) ingests data from a source system (like Salesforce) into a Databricks table.
# MAGIC
# MAGIC # How It Works
# MAGIC Represents External Systems: It first creates metadata objects in Unity Catalog to represent the external Salesforce table and the Qlik ingestion job itself.
# MAGIC
# MAGIC - Creates Lineage: It then establishes lineage relationships connecting these objects: Salesforce Table → Qlik Job → Databricks Table.
# MAGIC
# MAGIC - Idempotent Execution: The script is designed to be run repeatedly.
# MAGIC
# MAGIC - First Run: It creates all the necessary metadata and lineage records.
# MAGIC
# MAGIC - Subsequent Runs: It recognizes that the objects already exist and, instead of creating duplicates, it updates a last_run_utc timestamp on the Qlik job metadata. This acts as a record of the most recent data refresh.
# MAGIC
# MAGIC The result is a complete, end-to-end lineage graph visible in the Databricks Catalog Explorer, which accurately reflects when the data was last updated.

# COMMAND ----------

# DBTITLE 1,Setup demo table
# MAGIC %sql
# MAGIC create catalog if not exists prasad_kona_dev;
# MAGIC create schema if not exists prasad_kona_dev.salesforce;
# MAGIC CREATE or replace TABLE prasad_kona_dev.salesforce.salesforce_account (
# MAGIC     AccountNumber STRING,
# MAGIC     AccountSource STRING,
# MAGIC     AnnualRevenue DOUBLE,
# MAGIC     BillingAddress STRING,
# MAGIC     BillingCity STRING,
# MAGIC     BillingCountry STRING,
# MAGIC     BillingCountryCode STRING,
# MAGIC     BillingGeocodeAccuracy STRING,
# MAGIC     BillingLatitude DOUBLE,
# MAGIC     BillingLongitude DOUBLE,
# MAGIC     BillingPostalCode STRING,
# MAGIC     BillingState STRING,
# MAGIC     BillingStateCode STRING,
# MAGIC     BillingStreet STRING,
# MAGIC     ChannelProgramLevelName STRING,
# MAGIC     ChannelProgramName STRING,
# MAGIC     CleanStatus STRING,
# MAGIC     Description STRING,
# MAGIC     DunsNumber STRING,
# MAGIC     Fax STRING,
# MAGIC     Industry STRING,
# MAGIC     IsBuyer BOOLEAN,
# MAGIC     IsCustomerPortal BOOLEAN,
# MAGIC     IsPartner BOOLEAN,
# MAGIC     IsPersonAccount BOOLEAN,
# MAGIC     Jigsaw STRING,
# MAGIC     LastActivityDate DATE,
# MAGIC     LastReferencedDate TIMESTAMP,
# MAGIC     LastViewedDate TIMESTAMP,
# MAGIC     MasterRecordId STRING,
# MAGIC     NaicsCode STRING,
# MAGIC     NaicsDesc STRING,
# MAGIC     Name STRING,
# MAGIC     NumberOfEmployees INT,
# MAGIC     OwnerId STRING,
# MAGIC     Ownership STRING,
# MAGIC     ParentId STRING,
# MAGIC     Phone STRING,
# MAGIC     PhotoUrl STRING,
# MAGIC     Rating STRING,
# MAGIC     RecordTypeId STRING,
# MAGIC     ShippingAddress STRING,
# MAGIC     ShippingCity STRING,
# MAGIC     ShippingCountry STRING,
# MAGIC     ShippingCountryCode STRING,
# MAGIC     ShippingGeocodeAccuracy STRING,
# MAGIC     ShippingLatitude DOUBLE,
# MAGIC     ShippingLongitude DOUBLE,
# MAGIC     ShippingPostalCode STRING,
# MAGIC     ShippingState STRING,
# MAGIC     ShippingStateCode STRING,
# MAGIC     ShippingStreet STRING,
# MAGIC     Sic STRING,
# MAGIC     SicDesc STRING,
# MAGIC     Site STRING,
# MAGIC     TickerSymbol STRING,
# MAGIC     Tradestyle STRING,
# MAGIC     Type STRING,
# MAGIC     Website STRING,
# MAGIC     YearStarted STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC INSERT INTO prasad_kona_dev.salesforce.salesforce_account (
# MAGIC
# MAGIC     AccountNumber,
# MAGIC     AccountSource,
# MAGIC     AnnualRevenue,
# MAGIC     BillingAddress,
# MAGIC     BillingCity,
# MAGIC     BillingCountry,
# MAGIC     BillingCountryCode,
# MAGIC     BillingGeocodeAccuracy,
# MAGIC     BillingLatitude,
# MAGIC     BillingLongitude,
# MAGIC     BillingPostalCode,
# MAGIC     BillingState,
# MAGIC     BillingStateCode,
# MAGIC     BillingStreet,
# MAGIC     ChannelProgramLevelName,
# MAGIC     ChannelProgramName,
# MAGIC     CleanStatus,
# MAGIC     Description,
# MAGIC     DunsNumber,
# MAGIC     Fax,
# MAGIC     Industry,
# MAGIC     IsBuyer,
# MAGIC     IsCustomerPortal,
# MAGIC     IsPartner,
# MAGIC     IsPersonAccount,
# MAGIC     Jigsaw,
# MAGIC     LastActivityDate,
# MAGIC     LastReferencedDate,
# MAGIC     LastViewedDate,
# MAGIC     MasterRecordId,
# MAGIC     NaicsCode,
# MAGIC     NaicsDesc,
# MAGIC     Name,
# MAGIC     NumberOfEmployees,
# MAGIC     OwnerId,
# MAGIC     Ownership,
# MAGIC     ParentId,
# MAGIC     Phone,
# MAGIC     PhotoUrl,
# MAGIC     Rating,
# MAGIC     RecordTypeId,
# MAGIC     ShippingAddress,
# MAGIC     ShippingCity,
# MAGIC     ShippingCountry,
# MAGIC     ShippingCountryCode,
# MAGIC     ShippingGeocodeAccuracy,
# MAGIC     ShippingLatitude,
# MAGIC     ShippingLongitude,
# MAGIC     ShippingPostalCode,
# MAGIC     ShippingState,
# MAGIC     ShippingStateCode,
# MAGIC     ShippingStreet,
# MAGIC     Sic,
# MAGIC     SicDesc,
# MAGIC     Site,
# MAGIC     TickerSymbol,
# MAGIC     Tradestyle,
# MAGIC     Type,
# MAGIC     Website,
# MAGIC     YearStarted
# MAGIC )
# MAGIC VALUES
# MAGIC -- 1
# MAGIC ('AC-1001', 'Web', 5000000, '123 Main St', 'San Francisco', 'USA', 'US', 'ROOFTOP', 37.7749, -122.4194, '94105', 'CA', 'CA', '123 Main St', 'Gold', 'Partner Program', 'Clean', 'Main account for West Coast', '123456789', '555-123-4567', 'Technology', true, false, true, false, 'JIG-001', '2025-06-01', '2025-07-01 10:00:00', '2025-07-08 09:00:00', NULL, '541511', 'Custom Software', 'Acme Corp', 250, '0051U000007abcQ', 'Private', NULL, '555-987-6543', '/photo/acme.png', 'Hot', '0121U000000xyzA', '456 Market St', 'San Francisco', 'USA', 'US', 'ROOFTOP', 37.7750, -122.4195, '94105', 'CA', 'CA', '456 Market St', '7372', 'Software', 'acme.com', 'ACME', 'Direct', 'Customer - Direct', 'www.acme.com', '1999'),
# MAGIC -- 2
# MAGIC ('AC-1002', 'Phone Inquiry', 12000000, '789 Elm St', 'New York', 'USA', 'US', 'ROOFTOP', 40.7128, -74.0060, '10001', 'NY', 'NY', '789 Elm St', 'Silver', 'Growth Program', 'Clean', 'East Coast branch', '987654321', '555-222-3333', 'Finance', false, false, false, false, 'JIG-002', '2025-05-15', '2025-07-02 11:00:00', '2025-07-08 09:10:00', NULL, '522110', 'Commercial Banking', 'Beta Bank', 1200, '0051U000007defR', 'Public', NULL, '555-444-5555', '/photo/beta.png', 'Warm', '0121U000000xyzB', '101 Wall St', 'New York', 'USA', 'US', 'ROOFTOP', 40.7130, -74.0062, '10001', 'NY', 'NY', '101 Wall St', '6021', 'Banking', 'betabank.com', 'BETA', 'Direct', 'Customer - Direct', 'www.betabank.com', '1985'),
# MAGIC -- 3
# MAGIC ('AC-1003', 'Partner Referral', 800000, '321 Oak Ave', 'Chicago', 'USA', 'US', 'ROOFTOP', 41.8781, -87.6298, '60601', 'IL', 'IL', '321 Oak Ave', 'Bronze', 'Startup Program', 'Pending', 'Midwest startup', '555666777', '555-333-4444', 'Healthcare', false, false, false, false, 'JIG-003', '2025-04-10', '2025-07-03 12:00:00', '2025-07-08 09:20:00', NULL, '621111', 'Offices of Physicians', 'Gamma Health', 50, '0051U000007ghiS', 'Private', NULL, '555-666-7777', '/photo/gamma.png', 'Cold', '0121U000000xyzC', '654 Pine St', 'Chicago', 'USA', 'US', 'ROOFTOP', 41.8782, -87.6299, '60601', 'IL', 'IL', '654 Pine St', '8011', 'Healthcare', 'gammahealth.com', 'GAMMA', 'Channel', 'Customer - Channel', 'www.gammahealth.com', '2015'),
# MAGIC -- 4
# MAGIC ('AC-1004', 'Web', 2500000, '987 Maple Rd', 'Austin', 'USA', 'US', 'ROOFTOP', 30.2672, -97.7431, '73301', 'TX', 'TX', '987 Maple Rd', 'Gold', 'Enterprise Program', 'Clean', 'Texas enterprise', '888999000', '555-777-8888', 'Manufacturing', false, false, false, false, 'JIG-004', '2025-03-20', '2025-07-04 13:00:00', '2025-07-08 09:30:00', NULL, '333999', 'Industrial Machinery', 'Delta Manufacturing', 400, '0051U000007jklT', 'Private', NULL, '555-888-9999', '/photo/delta.png', 'Hot', '0121U000000xyzD', '321 Cedar St', 'Austin', 'USA', 'US', 'ROOFTOP', 30.2673, -97.7432, '73301', 'TX', 'TX', '321 Cedar St', '3561', 'Machinery', 'deltamfg.com', 'DELTA', 'Direct', 'Customer - Direct', 'www.deltamfg.com', '2005'),
# MAGIC -- 5
# MAGIC ('AC-1005', 'Trade Show', 15000000, '654 Spruce Ln', 'Seattle', 'USA', 'US', 'ROOFTOP', 47.6062, -122.3321, '98101', 'WA', 'WA', '654 Spruce Ln', 'Silver', 'Growth Program', 'Clean', 'Pacific Northwest branch', '222333444', '555-999-0000', 'Retail', false, false, false, false, 'JIG-005', '2025-02-28', '2025-07-05 14:00:00', '2025-07-08 09:40:00', NULL, '445110', 'Supermarkets', 'Epsilon Retail', 900, '0051U000007mnoU', 'Public', NULL, '555-000-1111', '/photo/epsilon.png', 'Warm', '0121U000000xyzE', '789 Birch St', 'Seattle', 'USA', 'US', 'ROOFTOP', 47.6063, -122.3322, '98101', 'WA', 'WA', '789 Birch St', '5411', 'Retail', 'epsilonretail.com', 'EPSI', 'Channel', 'Customer - Channel', 'www.epsilonretail.com', '1990'),
# MAGIC -- 6
# MAGIC ('AC-1006', 'Web', 3000000, '111 Willow Dr', 'Denver', 'USA', 'US', 'ROOFTOP', 39.7392, -104.9903, '80201', 'CO', 'CO', '111 Willow Dr', 'Bronze', 'Startup Program', 'Pending', 'Rocky Mountain region', '333444555', '555-222-1111', 'Energy', false, false, false, false, 'JIG-006', '2025-01-15', '2025-07-06 15:00:00', '2025-07-08 09:50:00', NULL, '221122', 'Electric Power', 'Zeta Energy', 300, '0051U000007pqrV', 'Private', NULL, '555-333-2222', '/photo/zeta.png', 'Cold', '0121U000000xyzF', '222 Aspen St', 'Denver', 'USA', 'US', 'ROOFTOP', 39.7393, -104.9904, '80201', 'CO', 'CO', '222 Aspen St', '4911', 'Energy', 'zetaenergy.com', 'ZETA', 'Direct', 'Customer - Direct', 'www.zetaenergy.com', '2010'),
# MAGIC -- 7
# MAGIC ('AC-1007', 'Email', 700000, '222 Poplar Ct', 'Miami', 'USA', 'US', 'ROOFTOP', 25.7617, -80.1918, '33101', 'FL', 'FL', '222 Poplar Ct', 'Gold', 'Partner Program', 'Clean', 'Florida branch', '444555666', '555-444-3333', 'Hospitality', false, false, false, false, 'JIG-007', '2025-06-10', '2025-07-07 16:00:00', '2025-07-08 10:00:00', NULL, '721110', 'Hotels', 'Eta Hospitality', 120, '0051U000007stuW', 'Private', NULL, '555-555-6666', '/photo/eta.png', 'Hot', '0121U000000xyzG', '333 Palm St', 'Miami', 'USA', 'US', 'ROOFTOP', 25.7618, -80.1919, '33101', 'FL', 'FL', '333 Palm St', '7011', 'Hospitality', 'etahospitality.com', 'ETA', 'Channel', 'Customer - Channel', 'www.etahospitality.com', '2018'),
# MAGIC -- 8
# MAGIC ('AC-1008', 'Web', 9500000, '333 Cedar Ave', 'Boston', 'USA', 'US', 'ROOFTOP', 42.3601, -71.0589, '02108', 'MA', 'MA', '333 Cedar Ave', 'Silver', 'Growth Program', 'Clean', 'Northeast branch', '555666888', '555-777-2222', 'Education', false, false, false, false, 'JIG-008', '2025-05-05', '2025-07-08 08:00:00', '2025-07-08 10:10:00', NULL, '611310', 'Colleges', 'Theta University', 2000, '0051U000007vwxX', 'Public', NULL, '555-888-3333', '/photo/theta.png', 'Warm', '0121U000000xyzH', '444 Oak St', 'Boston', 'USA', 'US', 'ROOFTOP', 42.3602, -71.0590, '02108', 'MA', 'MA', '444 Oak St', '8221', 'Education', 'thetauniversity.edu', 'THETA', 'Direct', 'Customer - Direct', 'www.thetauniversity.edu', '1850'),
# MAGIC -- 9
# MAGIC ('AC-1009', 'Web', 400000, '444 Aspen Blvd', 'Portland', 'USA', 'US', 'ROOFTOP', 45.5051, -122.6750, '97201', 'OR', 'OR', '444 Aspen Blvd', 'Bronze', 'Startup Program', 'Pending', 'Pacific branch', '666777888', '555-111-2222', 'Logistics', false, false, false, false, 'JIG-009', '2025-04-25', '2025-07-08 07:00:00', '2025-07-08 10:20:00', NULL, '488510', 'Freight Transportation', 'Iota Logistics', 80, '0051U000007yzabY', 'Private', NULL, '555-222-3334', '/photo/iota.png', 'Cold', '0121U000000xyzI', '555 Fir St', 'Portland', 'USA', 'US', 'ROOFTOP', 45.5052, -122.6751, '97201', 'OR', 'OR', '555 Fir St', '4731', 'Logistics', 'iotalogistics.com', 'IOTA', 'Channel', 'Customer - Channel', 'www.iotalogistics.com', '2017'),
# MAGIC -- 10
# MAGIC ('AC-1010', 'Web', 6000000, '555 Redwood Dr', 'San Diego', 'USA', 'US', 'ROOFTOP', 32.7157, -117.1611, '92101', 'CA', 'CA', '555 Redwood Dr', 'Gold', 'Enterprise Program', 'Clean', 'Southern California branch', '777888999', '555-333-5555', 'Biotech', false, false, false, false, 'JIG-010', '2025-03-10', '2025-07-08 06:00:00', '2025-07-08 10:30:00', NULL, '541711', 'Biotech Research', 'Kappa Biotech', 350, '0051U000007cdefZ', 'Private', NULL, '555-444-5556', '/photo/kappa.png', 'Hot', '0121U000000xyzJ', '666 Sequoia St', 'San Diego', 'USA', 'US', 'ROOFTOP', 32.7158, -117.1612, '92101', 'CA', 'CA', '666 Sequoia St', '2836', 'Biotech', 'kappabiotech.com', 'KAPPA', 'Direct', 'Customer - Direct', 'www.kappabiotech.com', '2008');
# MAGIC

# COMMAND ----------

# DBTITLE 1,Code to add lineage
import requests
import json
from datetime import datetime, timezone

# --- Configuration ---
# Your Databricks workspace URL.
DATABRICKS_HOST = "https://<your-workspace-url>"
DATABRICKS_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

# It's recommended to use dbutils to get the token in a notebook.
# If running externally, replace this with a Personal Access Token.
TOKEN = dbutils.entry_point.getDbutils().notebook().getContext().apiToken().get()


# The full, three-level name of the target table created by Qlik.
TARGET_TABLE_FULL_NAME = "prasad_kona_dev.salesforce.salesforce_account"

# --- API Helper Functions ---

def make_api_request(method, endpoint, payload=None):
    """A more robust helper to make requests and handle specific HTTP errors."""
    url = f"{DATABRICKS_HOST}/api/2.0/lineage-tracking/{endpoint}"
    headers = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}
    
    try:
        response = requests.request(method, url, headers=headers, data=json.dumps(payload) if payload else None)
        # We will handle status checks in the calling function
        return response
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

# --- Core Logic Functions ---

def create_or_update_metadata(metadata_def):
    """
    Gets, creates, or updates an external metadata object.
    This function is idempotent.
    """
    name = metadata_def["name"]
    print(f"Checking for external metadata object: '{name}'...")
    
    # 1. Try to get the object
    response = make_api_request("GET", f"external-metadata/{name}")
    
    if response.status_code == 200:
        print(f"'{name}' already exists. Updating its properties...")
        # 2. If it exists, update it with a PATCH
        update_payload = metadata_def.copy()
        # The name must be in the body for the update call
        update_payload["name"] = name
        
        # Build the update_mask from all keys in the original definition
        update_mask = ",".join(metadata_def.keys())
        
        patch_response = make_api_request(
            "PATCH", 
            f"external-metadata/{name}?update_mask={update_mask}", 
            payload=update_payload
        )
        patch_response.raise_for_status() # Ensure the PATCH was successful
        print(f"'{name}' updated successfully.")
        return patch_response.json()
        
    elif response.status_code == 404:
        print(f"'{name}' not found. Creating it...")
        # 3. If it doesn't exist, create it with a POST
        post_response = make_api_request("POST", "external-metadata", payload=metadata_def)
        post_response.raise_for_status() # Ensure the POST was successful
        print(f"'{name}' created successfully.")
        return post_response.json()
    else:
        # For other errors, raise an exception
        print(f"API Error getting '{name}': {response.status_code} - {response.text}")
        response.raise_for_status()


def create_lineage_if_not_exists(lineage_def):
    """
    Creates an external lineage relationship.
    Handles the case where the lineage already exists.
    """
    source_name = lineage_def["source"].get("external_metadata", {}).get("name")
    target_name = lineage_def["target"].get("table", {}).get("name") or lineage_def["target"].get("external_metadata", {}).get("name")
    
    print(f"Attempting to create lineage from '{source_name}' to '{target_name}'...")
    
    response = make_api_request("POST", "external-lineage", payload=lineage_def)
    
    if response.status_code == 200:
        print("Lineage created successfully.")
        return response.json()
    elif response.status_code == 400 and "already exists" in response.text:
        # This error message may vary, adjust if needed
        print("Lineage relationship already exists. Skipping creation.")
        return None
    else:
        print(f"API Error creating lineage: {response.status_code} - {response.text}")
        response.raise_for_status()


def add_or_update_lineage_for_run():
    """
    Main function to run for each data update.
    It ensures all metadata and lineage components exist and are up-to-date.
    """
    print("Starting lineage assertion process for Qlik ingestion run...")
    
    # --- Define Metadata Objects ---
    # This object is static and represents the source system.
    sfdc_metadata = {
        "name": "salesforce_accounts_table",
        "system_type": "SALESFORCE",
        "entity_type": "Table",
        "description": "Represents the 'accounts' table ingested from Salesforce."
    }
    
    # This object represents the Qlik job. We add a timestamp to track the latest run.
    qlik_metadata = {
        "name": "qlik_sfdc_ingestion_job",
        "system_type": "OTHER",
        "entity_type": "Job",
        "description": "Represents the Qlik job that ingests Salesforce 'accounts'.",
        "properties": {
            "last_run_utc": datetime.now(timezone.utc).isoformat(),
            "source_system": "Qlik"
        }
    }
    
    # Step 1: Create or Update the metadata objects
    create_or_update_metadata(sfdc_metadata)
    create_or_update_metadata(qlik_metadata)
    
    # --- Define Lineage Relationships ---
    # Step 2: Create the lineage connections if they don't already exist
    lineage_sfdc_to_qlik = {
        "source": {"external_metadata": {"name": sfdc_metadata["name"]}},
        "target": {"external_metadata": {"name": qlik_metadata["name"]}}
    }
    create_lineage_if_not_exists(lineage_sfdc_to_qlik)
    
    lineage_qlik_to_databricks = {
        "source": {"external_metadata": {"name": qlik_metadata["name"]}},
        "target": {"table": {"name": TARGET_TABLE_FULL_NAME}}
    }
    create_lineage_if_not_exists(lineage_qlik_to_databricks)
    
    print("\n✅ Lineage assertion complete! The 'last_run_utc' property on the Qlik job has been updated.")


# --- Run the process ---
# Replace with your actual catalog and schema names before running.
if "catalog_name" in TARGET_TABLE_FULL_NAME or "schema_name" in TARGET_TABLE_FULL_NAME:
    print("❗️ **Action Required** ❗️")
    print("Please update the 'TARGET_TABLE_FULL_NAME' variable with your actual catalog and schema names before running.")
else:
    # This is the function you would call each time your Qlik job runs.
    add_or_update_lineage_for_run()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The Source on the lineage graph : Salesforce
# MAGIC <br>
# MAGIC <img src="https://github.com/prasadkona/databricks_demos/blob/main/images/prasad-databricks-uc-byol-image1.jpeg?raw=true" width="600px" style="float:right"/>
# MAGIC <br>
# MAGIC The relation on the lineage graph : Qlik
# MAGIC <br>
# MAGIC <img src="https://github.com/prasadkona/databricks_demos/blob/main/images/prasad-databricks-uc-byol-image2.jpeg?raw=true" width="600px" style="float:right"/>
# MAGIC
# MAGIC <img src="https://github.com/prasadkona/databricks_demos/blob/main/images/prasad-databricks-uc-byol-image3.jpeg?raw=true" width="600px" style="float:right"/>
# MAGIC <br>
# MAGIC The target on the lineage graph : Databricks table
# MAGIC <br>
# MAGIC <img src="https://github.com/prasadkona/databricks_demos/blob/main/images/prasad-databricks-uc-byol-image4.jpeg?raw=true" width="600px" style="float:right"/>
# MAGIC
# MAGIC <img src="https://github.com/prasadkona/databricks_demos/blob/main/images/prasad-databricks-uc-byol-image5.jpeg?raw=true" width="600px" style="float:right"/>