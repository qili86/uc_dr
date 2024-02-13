# Databricks notebook source
import re
from pyspark.sql.functions import col, when, collect_list, upper,concat_ws,col

# COMMAND ----------

def get_deleted_catalog(primary_catalog_df):
    primary_catalog_list = []
    secondary_catalog_list = []
    secondary_catalogs_df = spark.sql("select * from system.information_schema.catalogs").filter("catalog_owner<>'System user'")
    for catalog in primary_catalog_df.collect():
        primary_catalog_list.append(catalog.catalog_name)
    for catalog in secondary_catalogs_df.collect():
        secondary_catalog_list.append(catalog.catalog_name)   
    deleted_catalogs = [item for item in secondary_catalog_list if item not in primary_catalog_list]
    return deleted_catalogs

# COMMAND ----------

def get_deleted_schema(primary_schema_df):
    primary_schemas_list = []
    secondary_schemas_list = []
    secondary_schemas_df = spark.sql(f"select schema_name, catalog_name from system.information_schema.schemata").filter("schema_name<>'information_schema' and schema_owner<>'System user'")
    for schema in secondary_schemas_df.collect():
        secondary_schemas_list.append(f"{schema.catalog_name}.{schema.schema_name}")
    for schema in primary_schema_df.collect():
        primary_schemas_list.append(f"{schema.catalog_name}.{schema.schema_name}")
    deleted_schema = [item for item in secondary_schemas_list if item not in primary_schemas_list]
    return deleted_schema

# COMMAND ----------

def get_deleted_table(primary_table_df):
    primary_table_list= []
    secondary_table_list = []
    secondary_tables_df = spark.sql(f"select table_schema, table_name, table_catalog from system.information_schema.tables").filter("table_schema<>'information_schema' and table_type='EXTERNAL'")
    for table in primary_table_df.collect():
        primary_table_list.append(f"{table.table_catalog}.{table.table_schema}.{table.table_name}")
    for table in secondary_tables_df.collect():
        secondary_table_list.append(f"{table.table_catalog}.{table.table_schema}.{table.table_name}")   
    deleted_table = [item for item in secondary_table_list if item not in primary_table_list]
    return deleted_table

# COMMAND ----------

def get_schema_properties(properties_str):
    pairs = re.findall(r'\(([^,]+),([^)]+)\)', properties_str)
    # Convert key-value pairs to a dictionary
    converted_dict = {str(key).lstrip('('): str(value) for key, value in pairs}
    return converted_dict

# COMMAND ----------

def process_ct_stmt(ct_stmt):
    pattern = re.compile(r"TBLPROPERTIES\s*\([^)]+\)")
    ct_stmt = re.sub(pattern, "", ct_stmt)
    if is_delta(ct_stmt):
       ct_stmt = use_create_or_replace_for_delta(ct_stmt)
    else:
        ct_stmt = use_create_if_not_exist_for_non_delta(ct_stmt)
    return ct_stmt

# COMMAND ----------

def is_delta(ct_stmt):
    pattern = re.compile(r"(USING\s+[a-zA-Z0-9_]+\s*)")
    match = re.search(pattern, ct_stmt)
    if match:
        using_line = match.group(1)
        # print(using_line)
        if 'delta' in using_line:
           return True
        else:
           return False
    else:
        print(f"USING XXX line not found in the {ct_stmt}.")
        return False

# COMMAND ----------

def use_create_or_replace_for_delta(ct_stmt):
    pattern = re.compile(r"CREATE TABLE", re.IGNORECASE)
    modified_ct_stmt = re.sub(pattern, "CREATE OR REPLACE TABLE", ct_stmt, count=1)
    return modified_ct_stmt

# COMMAND ----------

def use_create_if_not_exist_for_non_delta(ct_stmt):
    pattern = re.compile(r"CREATE TABLE", re.IGNORECASE)
    modified_ct_stmt = re.sub(pattern, "CREATE TABLE IF NOT EXISTS", ct_stmt, count=1)
    return modified_ct_stmt

