# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("rootExternalStorage", "abfss://data@starbucksdev.dfs.core.windows.net/daibo/", "Root of external tables' path")

# COMMAND ----------

root_externalstorage =  dbutils.widgets.get("rootExternalStorage")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS uc_dr")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS uc_dr.test_cases")

# COMMAND ----------

# create an external delta table
spark.sql(f"create table if not exists uc_dr.test_cases.delta_table (c1 int, c2 string) using delta location '{root_externalstorage}uc_dr/test_cases/delta_table'")

# COMMAND ----------

# create an external parquet table
spark.sql(f"create table if not exists uc_dr.test_cases.parquet_table (c1 int, c2 string)using parquet location '{root_externalstorage}uc_dr/test_cases/parquet_table'")

# COMMAND ----------

# create an external partitioned table
spark.sql(f"create table if not exists uc_dr.test_cases.partitioned_table (c1 int, c2 string)using delta location '{root_externalstorage}uc_dr/test_cases/partitioned_table' partitioned by (c1)")

# COMMAND ----------

# create a hive serde table,  Data source format hive is not supported in Unity Catalog.
# spark.sql(f"create table if not exists uc_dr.test_cases.sync_hive (c1 int, c2 string) using hive location '{root_externalstorage}uc_dr/test_cases/hive_table'");

# COMMAND ----------


# create a bucketed table, csv, clustered
spark.sql(f"create table if not exists uc_dr.test_cases.bucketed_table (c1 int, c2 string) using csv location '{root_externalstorage}uc_dr/test_cases/bucketed_table' clustered by (c1) into 2 buckets")

# COMMAND ----------

import re
input_string = "((Create-by,kevin), (Create-date,09/01/2019),(update-date,09/01/2019))"
# Extract key-value pairs using a regular expression
pairs = re.findall(r'\(([^,]+),([^)]+)\)', input_string)
# Convert key-value pairs to a dictionary
converted_dict = {str(key).lstrip('('): str(value) for key, value in pairs}
print(converted_dict)
