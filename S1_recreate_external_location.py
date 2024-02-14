# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "abfss://system@eastus2external.dfs.core.windows.net/", "Storage with source catalog info")

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
print(storage_location)

# COMMAND ----------

external_locations_df = spark.read.format("delta").load(f"{storage_location}/external_locations") 

# COMMAND ----------

display(external_locations_df)

# COMMAND ----------

el_errors = []

for el in external_locations_df.filter("external_location_name <> 'metastore_default_location'").collect():
    stmt = f"""CREATE EXTERNAL LOCATION IF NOT EXISTS {el.external_location_name}
                 URL '{el.url}'
                 WITH (STORAGE CREDENTIAL {el.storage_credential_name})
                 COMMENT '{el.comment}'
                 """
    print(stmt)
    try:
        spark.sql(stmt)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        el_errors.append([external_location_name, f"An unexpected error occurred: {e}"])

if len(el_errors) > 0:
    el_errors_df = spark.createDataFrame(el_errors, ['external_location_name', 'error'])
    display(el_errors_df)

