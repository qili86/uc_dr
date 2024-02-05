# Databricks notebook source
# MAGIC %md
# MAGIC ##Backup System Information Schema
# MAGIC
# MAGIC This notebook will read from the information_schema of System schema and dump its contents to external storage.
# MAGIC
# MAGIC This external storage will be independent from the UC storage and will be accessible by a remote workspace on a different region with a different UC.
# MAGIC
# MAGIC Assumptions:
# MAGIC - currently overwrites to the same folder

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "abfss://system@eastus2external.dfs.core.windows.net/", "Storage location for copy")

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
print(storage_location)

# COMMAND ----------

    table_list = spark.catalog.listTables("system.information_schema")
    for table in table_list:
        df = spark.sql(f"SELECT * FROM system.information_schema.{table.name}")
        df.write.format("delta").mode("overwrite").save(f"{storage_location}/{table.name}")
