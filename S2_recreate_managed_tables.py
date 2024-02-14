# Databricks notebook source
# MAGIC %md
# MAGIC ##Backup Managed Tables

# COMMAND ----------

is_primary_region = False 
dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "abfss://system@eastus2external.dfs.core.windows.net/", "Storage location for copy")

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
print(storage_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Backup Managed Tables

# COMMAND ----------

managed_tables_df = spark.read.format("delta").load(f"{storage_location}/tables").filter(f"table_schema<>'information_schema' and table_type='MANAGED' and table_owner <> 'System user' and table_catalog <> '__databricks_internal'")
managed_tables_df.display()

# COMMAND ----------

from pyspark.sql.functions import col, collect_list

grouped_managed_tables_df = managed_tables_df.groupBy("table_catalog", "table_schema").agg(collect_list("table_name").alias("tables"))
grouped_managed_tables_df.display()

to_replicate = {}

for item in grouped_managed_tables_df.collect():
    if item.table_catalog not in to_replicate:
      to_replicate[item.table_catalog] = {item.table_schema: item.tables}
    else:
        to_replicate[item.table_catalog][item.table_schema] = item.tables

print(to_replicate)


# COMMAND ----------

clone_catalog = 'uc_dr_managed_tables_clone'
ext_location_url = f"{storage_location}/uc_dr_managed_tables/"
param_names = {"dest_catalog"}

# COMMAND ----------

csql = f"CREATE CATALOG IF NOT EXISTS {clone_catalog} MANAGED LOCATION '{ext_location_url}'"
spark.sql(csql)

# COMMAND ----------

import re
name_cleanup_re = re.compile(r"[\W-]")

def gen_schema_name(catalog: str, schema: str):
  """Generates a schema name inside the clones catalog by concatenating src catalog + schema
  """
  cat_name = re.sub(name_cleanup_re, "_", catalog)
  schema_name = re.sub(name_cleanup_re, "_", schema)
  return f"{cat_name}_{schema_name}"

def gen_table_dir(catalog: str, schema: str, table: str):
  """Generates directory name for a cloned table
  """
  table_name = re.sub(name_cleanup_re, "_", table)
  return f"{gen_schema_name(catalog, schema)}/{table_name}/"

# COMMAND ----------

for catalog, scm in to_replicate.items():
  dest_catalog = scm.get("dest_catalog", catalog)
  if not is_primary_region:
    # Really we need to pre-create catalogs via terraform or something like that to simplify specification of locations 
    spark.sql(f"create catalog if not exists `{dest_catalog}`")
  for schema, tables in scm.items():
    if schema in param_names:
      continue
    schema_name = gen_schema_name(catalog, schema)
    if not is_primary_region:
      # We may need to pre-create schemas that to simplify specification of locations, or find a way how to specify them in the target region
      spark.sql(f"create schema if not exists `{dest_catalog}`.{schema}")
      spark.sql(f"create schema if not exists `{clone_catalog}`.{schema_name}")
    else:
      # Create schema in the clones catalog
      spark.sql(f"create schema if not exists `{clone_catalog}`.{schema_name}")
    for table in tables:
      print(f"Processing {catalog}.{schema}.{table}")
      # TODO: we'll need to gracefully handle if table doesn't exist - it would be easier if we take tables to clone from the information_schema of catalogs
      table_dir = f"{ext_location_url}{gen_table_dir(catalog, schema, table)}"
      if is_primary_region:
        # clone the table into the clone external location
        sql = f"""create or replace table `{clone_catalog}`.`{schema_name}`.`{table}`
        deep clone `{catalog}`.`{schema}`.`{table}` location '{table_dir}'"""
        print("Executing", sql)
        spark.sql(sql)
      else:
        sql = f"""create table if not exists `{clone_catalog}`.`{schema_name}`.`{table}`
        location '{table_dir}'"""
        print("Executing", sql)
        spark.sql(sql)
        sql = f"""create or replace table `{dest_catalog}`.`{schema}`.`{table}`
        deep clone `{clone_catalog}`.`{schema_name}`.`{table}`"""
        print("Executing", sql)
        spark.sql(sql)
