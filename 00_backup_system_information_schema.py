# Databricks notebook source
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
