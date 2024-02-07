# Databricks notebook source
# MAGIC %md
# MAGIC ##Re-create catalog, schemas and external tables at a remote UC
# MAGIC
# MAGIC This notebook will read from an external container with a backup from a source catalog and re-create it.
# MAGIC
# MAGIC If run on a DR site, the information_schema parameter should be the same as the source catalog.
# MAGIC
# MAGIC Assumptions:
# MAGIC - The storage credential(s) and external location(s) of the parent external location needs to be created on the target UC beforehand.
# MAGIC - The external location for all schemas is the same, formed of ```<storage location root>/<schema name>/<table name>```
# MAGIC - All tables are Delta

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "/mnt/externallocation", "Storage with source catalog info")
dbutils.widgets.text("catalogName", "system", "information_schema catalog")
dbutils.widgets.text("rootExternalStorage", "abfss://externaltables@dspadottodrstaging.dfs.core.windows.net/root/", "Root of external tables' path")

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
catalog_name =  dbutils.widgets.get("catalogName")
root_externalstorage =  dbutils.widgets.get("rootExternalStorage")

#table_list = dbutils.fs.ls(storage_location)

# COMMAND ----------

import re
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

# MAGIC %md
# MAGIC #### Create catalog

# COMMAND ----------

catalog_df = spark.read.format("delta").load(f"{storage_location}/catalogs")

#Change to this iteration when running on a remote UC
for catalog in catalog_df.collect():     
    spark.sql(f"CREATE CATALOG {catalog.catalog_name} COMMENT '{catalog.comment}'")
    spark.sql(f"ALTER CATALOG {catalog.catalog_name} SET OWNER to `{catalog.catalog_owner}`")

#Local testing with different catalog name
#spark.sql(f"CREATE CATALOG {catalog_name} COMMENT '{catalog_df.collect()[0].comment}'")
#spark.sql(f"ALTER CATALOG {catalog.catalog_name} SET OWNER to `{catalog_df.collect()[0].catalog_owner}`")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create schemas

# COMMAND ----------

from pyspark.sql.functions import col, when, collect_list, upper


#Get only user schemas
schemas_df = spark.read.format("delta").load(f"{storage_location}/schemata").filter("schema_name<>'information_schema'")

#Drop the default schema
spark.sql(f"DROP SCHEMA {catalog_name}.default")

#Create all user schemas on the target catalog
for schema in schemas_df.collect():    
    spark.sql(f"CREATE SCHEMA {catalog_name}.{schema.schema_name} COMMENT '{schema.comment}'")
    spark.sql(f"ALTER SCHEMA {catalog_name}.{schema.schema_name} SET OWNER to `{schema.schema_owner}`")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create external tables

# COMMAND ----------

#Get only external user tables
tables_df = spark.read.format("delta").load(f"{storage_location}/tables").filter("table_schema<>'information_schema' and table_type='EXTERNAL'")

for table in tables_df.collect():
    columns_df = spark.read.format("delta").load(f"{storage_location}/columns").filter((col("table_schema") == table.table_schema) & (col("table_name") == table.table_name))
    columns = return_schema(columns_df)

    #Create Table
    spark.sql(f"CREATE OR REPLACE TABLE {catalog_name}.{table.table_schema}.{table.table_name}({columns}) COMMENT '{table.comment}' LOCATION '{root_externalstorage}{table.table_schema}/{table.table_name}'")
    spark.sql(f"ALTER TABLE {catalog_name}.{table.table_schema}.{table.table_name} SET OWNER to `{table.table_owner}`")
