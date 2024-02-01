# Databricks notebook source
# MAGIC %md
# MAGIC ##Backup catalog
# MAGIC
# MAGIC This notebook will read from the information_schema of a given catalog and dump its contents to external storage.
# MAGIC
# MAGIC This external storage will be independent from the UC storage and will be accessible by a remote workspace on a different region with a different UC.
# MAGIC
# MAGIC Assumptions:
# MAGIC - Dump and restore one catalog at a time (currently overwrites to the same folder)

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "abfss://metadata@eastus2external.dfs.core.windows.net/", "Storage location for copy")
storage_location = dbutils.widgets.get("storageLocation")

# COMMAND ----------

all_catalogs = spark.sql("SHOW CATALOGS").filter("catalog<>'hive_metastore' and catalog<>'system' and catalog<>'samples' and catalog <>'__databricks_internal'").collect()


# COMMAND ----------


for catalog in all_catalogs:
    catalog_name = catalog.catalog
    print(catalog_name)
    table_list = spark.catalog.listTables(f"{catalog_name}.information_schema")
    for table in table_list:
        df = spark.sql(f"SELECT * FROM {table.catalog}.information_schema.{table.name}")
        df.write.format("delta").mode("overwrite").save(f"{storage_location}/{catalog_name}/{table.name}")

# COMMAND ----------

table_location = []
catalogs = []
for catalog in all_catalogs:
    catalog_name = catalog.catalog
    catalogs.append([catalog_name])
    tables_df = spark.read.format("delta").load(f"{storage_location}/{catalog_name}/tables").filter("table_schema<>'information_schema' and table_type='EXTERNAL'")
    for table in tables_df.collect():
        print(f"{table.table_schema}.{table.table_name}")
        location = spark.sql(f"DESCRIBE EXTENDED {catalog_name}.{table.table_schema}.{table.table_name}").filter("col_name == 'Location'").first()['data_type']
        table_location.append([f"{catalog_name}.{table.table_schema}.{table.table_name}", location])
table_location_df = spark.createDataFrame(table_location, ['full_name', 'location'])
display(table_location_df)
catalogs_df = spark.createDataFrame(catalogs, ['catalog'])
display(catalogs_df)
table_location_df.write.format("delta").mode("overwrite").save(f"{storage_location}/uc_dr_table_locations")
catalogs_df.write.format("delta").mode("overwrite").save(f"{storage_location}/uc_dr_catalogs")
