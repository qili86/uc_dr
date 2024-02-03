# Databricks notebook source
import re
from pyspark.sql.functions import col, when, collect_list, upper,concat_ws,col

# COMMAND ----------

def return_schema(df):
    column_names = df.orderBy(df.ordinal_position.asc()).select("column_name", upper("full_data_type")).collect()
    schema = ""
    for x,y in column_names:
        sql =f''' {x} {y},'''
        schema += sql
        if y == []:
            break

    p = re.compile('(,$)')
    schema_no_ending_comma = p.sub('', schema)
    return(schema_no_ending_comma)

# COMMAND ----------

def get_deleted_catalog(primary_catalog_df):
    primary_catalog_list = []
    secondary_catalog_list = []
    secondary_catalogs_df = spark.sql("SHOW CATALOGS").filter("catalog<>'hive_metastore' and catalog<>'system' and catalog<>'samples' and catalog <>'__databricks_internal'")
    for catalog in primary_catalog_df.collect():
        primary_catalog_list.append(catalog.catalog)
    for catalog in secondary_catalogs_df.collect():
        secondary_catalog_list.append(catalog.catalog)   
    deleted_catalogs = [item for item in secondary_catalog_list if item not in primary_catalog_list]
    return deleted_catalogs

# COMMAND ----------

def get_deleted_schema(catalog_name, primary_schema_df):
    primary_schemas_list = []
    secondary_schemas_list = []
    secondary_schemas_df = spark.sql(f"select schema_name, catalog_name from {catalog_name}.information_schema.schemata").filter("schema_name<>'information_schema'")
    for schema in secondary_schemas_df.collect():
        secondary_schemas_list.append(f"{schema.catalog_name}.{schema.schema_name}")
    for schema in primary_schema_df.collect():
        primary_schemas_list.append(f"{schema.catalog_name}.{schema.schema_name}")
    deleted_schema = [item for item in secondary_schemas_list if item not in primary_schemas_list]
    return deleted_schema

# COMMAND ----------

def get_deleted_table(catalog_name, primary_table_df):
    primary_table_list= []
    secondary_table_list = []
    secondary_tables_df = spark.sql(f"select table_schema, table_name, table_catalog from {catalog_name}.information_schema.tables").filter("table_schema<>'information_schema' and table_type='EXTERNAL'")
    for table in primary_table_df.collect():
        primary_table_list.append(f"{table.table_catalog}.{table.table_schema}.{table.table_name}")
    for table in secondary_tables_df.collect():
        secondary_table_list.append(f"{table.table_catalog}.{table.table_schema}.{table.table_name}")   
    deleted_table = [item for item in secondary_table_list if item not in primary_table_list]
    return deleted_table
