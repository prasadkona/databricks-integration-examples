# Databricks notebook source
# MAGIC %md 
# MAGIC ## Primary key / Foreign key
# MAGIC
# MAGIC - https://docs.databricks.com/en/tables/constraints.html
# MAGIC - https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-constraint.html

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog prasad_kona_dev;
# MAGIC use schema default;
# MAGIC CREATE TABLE T(pk1 INTEGER NOT NULL, pk2 INTEGER NOT NULL,
# MAGIC                 CONSTRAINT t_pk PRIMARY KEY(pk1, pk2));
# MAGIC CREATE TABLE S(pk INTEGER NOT NULL PRIMARY KEY,
# MAGIC                 fk1 INTEGER, fk2 INTEGER,
# MAGIC                 CONSTRAINT s_t_fk FOREIGN KEY(fk1, fk2) REFERENCES T);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog prasad_kona_dev;
# MAGIC use schema default;
# MAGIC -- Create a table with a primary key
# MAGIC  CREATE or replace TABLE persons(first_name STRING NOT NULL, last_name STRING NOT NULL, nickname STRING,
# MAGIC                        CONSTRAINT persons_pk PRIMARY KEY(first_name, last_name));
# MAGIC
# MAGIC -- create a table with a foreign key
# MAGIC  CREATE  or replace TABLE pets(name STRING, owner_first_name STRING, owner_last_name STRING,
# MAGIC                     CONSTRAINT pets_persons_fk FOREIGN KEY (owner_first_name, owner_last_name) REFERENCES persons);
# MAGIC
# MAGIC -- Create a table with a single column primary key and system generated name
# MAGIC CREATE  or replace TABLE customers(customerid STRING NOT NULL PRIMARY KEY, name STRING);
# MAGIC
# MAGIC -- Create a table with a names single column primary key and a named single column foreign key
# MAGIC  CREATE  or replace TABLE orders(orderid BIGINT NOT NULL CONSTRAINT orders_pk PRIMARY KEY,
# MAGIC                       customerid STRING CONSTRAINT orders_customers_fk REFERENCES customers);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog prasad_kona_dev;
# MAGIC use schema default;
# MAGIC
# MAGIC drop table if exists persons2;
# MAGIC drop table if exists pets2;
# MAGIC
# MAGIC drop table if exists customers2;
# MAGIC drop table if exists orders2;

# COMMAND ----------

# DBTITLE 1,Create demo schemas with constraints (pk/fk)
# MAGIC %sql
# MAGIC
# MAGIC use catalog prasad_kona_dev;
# MAGIC use schema default;
# MAGIC
# MAGIC --- Example 1 ---
# MAGIC -- Create a table with a primary key
# MAGIC  CREATE  or replace TABLE persons2(first_name STRING NOT NULL, last_name STRING NOT NULL, nickname STRING,
# MAGIC                        CONSTRAINT persons2_pk PRIMARY KEY(first_name, last_name));
# MAGIC
# MAGIC -- create a table with a foreign key
# MAGIC  CREATE  or replace TABLE pets2(name STRING, owner_first_name STRING, owner_last_name STRING,
# MAGIC                     CONSTRAINT pets2_persons2_fk FOREIGN KEY (owner_first_name, owner_last_name) REFERENCES persons2 (first_name,last_name));
# MAGIC
# MAGIC
# MAGIC --- Example 2 ---
# MAGIC -- Create a table with a single column primary key and system generated name
# MAGIC CREATE  or replace TABLE customers2(customerid STRING NOT NULL PRIMARY KEY, name STRING);
# MAGIC
# MAGIC -- Create a table with a names single column primary key and a named single column foreign key
# MAGIC  CREATE  or replace TABLE orders2(orderid BIGINT NOT NULL CONSTRAINT orders2_pk PRIMARY KEY,
# MAGIC                       customerid STRING CONSTRAINT orders2_customers2_fk REFERENCES customers2(customerid));
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted prasad_kona_dev.default.pets2

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail prasad_kona_dev.default.pets2

# COMMAND ----------

# DBTITLE 1,Query to list all foreign key relationships for a specific table
# MAGIC %sql
# MAGIC -- Query to list all foreign key relationships for a specific table in the specified schema/catalog
# MAGIC
# MAGIC use catalog prasad_kona_dev; -- Replace with your actual catalog name
# MAGIC
# MAGIC select
# MAGIC   tc.constraint_type,
# MAGIC   rc.constraint_name,
# MAGIC   rc.unique_constraint_name,
# MAGIC   kcu_fk.table_catalog as child_catalog_name,
# MAGIC   kcu_fk.table_schema as child_schema_name,
# MAGIC   kcu_fk.table_name as parent_table_name,
# MAGIC   kcu_fk.column_name as parent_table_column_name,
# MAGIC   kcu_fk.ordinal_position,
# MAGIC   kcu_fk.position_in_unique_constraint,
# MAGIC   kcu_pk.table_catalog as parent_catalog_name,
# MAGIC   kcu_pk.table_schema as parent_schema_name,
# MAGIC   kcu_pk.table_name as parent_table_name,
# MAGIC   kcu_pk.column_name as parent_table_column_name
# MAGIC from
# MAGIC   information_schema.referential_constraints rc
# MAGIC   join information_schema.key_column_usage kcu_fk on rc.constraint_name = kcu_fk.constraint_name
# MAGIC   join information_schema.key_column_usage kcu_pk on rc.unique_constraint_name = kcu_pk.constraint_name
# MAGIC   and kcu_fk.position_in_unique_constraint = kcu_pk.ordinal_position
# MAGIC   join information_schema.table_constraints tc on rc.constraint_name = tc.constraint_name
# MAGIC
# MAGIC WHERE kcu_fk.table_schema = 'default' -- Specify the schema name here
# MAGIC     AND kcu_fk.table_name ='pets2' -- Replace with your actual table name
# MAGIC     AND tc.constraint_type = 'FOREIGN KEY'
# MAGIC ORDER BY kcu_fk.ordinal_position asc, kcu_fk.table_catalog, kcu_fk.table_schema, kcu_fk.table_name, kcu_fk.column_name;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Query to list primary key constraints for a specific table
# MAGIC %sql
# MAGIC
# MAGIC -- Query to list primary key constraints for a specific table in the specified schema/catalog
# MAGIC
# MAGIC use catalog prasad_kona_dev; -- Replace with your actual catalog name
# MAGIC
# MAGIC select
# MAGIC   tc.constraint_type as constraint_type,
# MAGIC   tc.constraint_name as constraint_name,
# MAGIC   tc.table_catalog as catalog_name,
# MAGIC   tc.table_schema as schema_name,
# MAGIC   tc.table_name as table_name,
# MAGIC   kcu.column_name as column_name,
# MAGIC   kcu.ordinal_position as ordinal_position
# MAGIC from
# MAGIC   information_schema.table_constraints tc
# MAGIC   join information_schema.key_column_usage kcu on tc.constraint_name = kcu.constraint_name
# MAGIC WHERE tc.table_schema = 'default' -- Specify the schema name here
# MAGIC     AND tc.table_name ='persons2' -- Replace with your actual table name
# MAGIC    AND tc.constraint_type = 'PRIMARY KEY'
# MAGIC ORDER BY kcu.ordinal_position asc, tc.table_catalog, tc.table_schema, tc.table_name, kcu.column_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog prasad_kona_dev;
# MAGIC use schema default;
# MAGIC insert into persons2 values ('f1','l1','nickname1'), ('f2','l2','nickname2');

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog prasad_kona_dev;
# MAGIC use schema default;
# MAGIC insert into pets2 values ('pet1','f1','l1'), ('pet2','f3','l3');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p2.*, pt2.name AS pet_name
# MAGIC FROM prasad_kona_dev.default.persons2 p2
# MAGIC JOIN prasad_kona_dev.default.pets2 pt2
# MAGIC   ON p2.first_name = pt2.owner_first_name 
# MAGIC   AND p2.last_name = pt2.owner_last_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Get table properties using rest api

# COMMAND ----------

# DBTITLE 1,Using Rest api to query for UC table metadata
import requests

def get_table_properties(full_table_name):
    access_token = dbutils.secrets.get(scope="prasad_kona", key="databricks_user_token")
    databricks_hostname = "e2-demo-field-eng.cloud.databricks.com"
    response = requests.get(f"https://{databricks_hostname}/api/2.1/unity-catalog/tables/{full_table_name}",
                            headers={"Authorization": f"Bearer {access_token}"})
    table_properties = response.json()
    return table_properties



# COMMAND ----------

# MAGIC %md
# MAGIC example showing extracting foreign key metadata

# COMMAND ----------

# Example usage
full_table_name = "prasad_kona_dev.default.pets2"
table_properties = get_table_properties(full_table_name)
print(table_properties)

# COMMAND ----------

# Extracting table_constraints
table_constraints = table_properties['table_constraints']
print(table_constraints)

# COMMAND ----------

# MAGIC %md
# MAGIC example showing extracting primary key metadata

# COMMAND ----------

# Example usage
full_table_name = "prasad_kona_dev.default.persons2"
table_properties = get_table_properties(full_table_name)
print(table_properties)

# COMMAND ----------

# Extracting table_constraints
table_constraints = table_properties['table_constraints']
print(table_constraints)
