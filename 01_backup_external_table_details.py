# Databricks notebook source
# MAGIC %md
# MAGIC ##Backup External Table Details
# MAGIC
# MAGIC This notebook will read from the information_schema of a given catalog and dump its contents to external storage.
# MAGIC
# MAGIC This external storage will be independent from the UC storage and will be accessible by a remote workspace on a different region with a different UC.
# MAGIC
# MAGIC Assumptions:
# MAGIC - Dump and restore one catalog at a time (currently overwrites to the same folder)

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "abfss://system@eastus2external.dfs.core.windows.net/", "Storage location for copy")

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
print(storage_location)

# COMMAND ----------

tables_df = spark.read.format("delta").load(f"{storage_location}/tables").filter("table_schema<>'information_schema' and table_type='EXTERNAL'")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType

tables_info = []
table_errors = []
tables_create_stmt = []

for table in tables_df.collect():
    name = f"{table.table_catalog}.{table.table_schema}.{table.table_name}"
    print(f"----scan details for external table: {name}")
    try:
        table_info = spark.sql(f"DESCRIBE DETAIL {name}").first()
        tables_info.append([name, table_info['location'], table_info['partitionColumns'], table_info['clusteringColumns'], table_info['properties'], table_info['format']])
        createtab_stmt = spark.sql(f"SHOW CREATE TABLE {name}").first()['createtab_stmt']
        tables_create_stmt.append([name, createtab_stmt])
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        table_errors.append([name, f"An unexpected error occurred: {e}"])

schema = StructType([
    StructField("full_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("partitionColumns", ArrayType(StringType()), True),
    StructField("clusteringColumns", ArrayType(StringType()), True),
    StructField("properties", MapType(StringType(), StringType()), True),
    StructField("format", StringType(), True)
])

tables_info_df = spark.createDataFrame(tables_info, schema)
tables_info_df.write.format("delta").mode("overwrite").save(f"{storage_location}/uc_dr_tables_details")

tables_create_stmt_df = spark.createDataFrame(tables_create_stmt, ['name', 'stmt'])
tables_create_stmt_df.write.format("delta").mode("overwrite").save(f"{storage_location}/uc_dr_tables_create_stmts")
display(tables_create_stmt_df)

if len(table_errors) > 0:
    table_errors_df = spark.createDataFrame(table_errors, ['table', 'error'])
    display(table_errors_df)
