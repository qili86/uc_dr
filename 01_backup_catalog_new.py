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
dbutils.widgets.text("catalogName", "", "catalog name, it is empty by default to run all catalog, set it to a specific catalog for testing")

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
catalog_name = dbutils.widgets.get("catalogName")
print(storage_location)
print(catalog_name)

# COMMAND ----------

if catalog_name:
    all_catalogs = spark.sql("SHOW CATALOGS").filter(f"catalog = '{catalog_name}'").collect()
else:
    all_catalogs = spark.sql("SHOW CATALOGS").filter("catalog<>'hive_metastore' and catalog<>'system' and catalog<>'samples' and catalog <>'__databricks_internal'").collect()

print(all_catalogs)

# COMMAND ----------

for catalog in all_catalogs:
    catalog_name = catalog.catalog
    print(catalog_name)
    table_list = spark.catalog.listTables(f"{catalog_name}.information_schema")
    for table in table_list:
        df = spark.sql(f"SELECT * FROM {table.catalog}.information_schema.{table.name}")
        df.write.format("delta").mode("overwrite").save(f"{storage_location}/{catalog_name}/{table.name}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType

tables_info = []
catalogs = []
table_errors = []
tables_create_stmt = []

for catalog in all_catalogs:
    catalog_name = catalog.catalog
    catalogs.append([catalog_name])
    tables_df = spark.read.format("delta").load(f"{storage_location}/{catalog_name}/tables").filter("table_schema<>'information_schema' and table_type='EXTERNAL'")
    for table in tables_df.collect():
        print(f"----scan details for external table: {catalog_name}.{table.table_schema}.{table.table_name}")
        try:
            table_info = spark.sql(f"DESCRIBE DETAIL {catalog_name}.{table.table_schema}.{table.table_name}").first()
            tables_info.append([f"{catalog_name}.{table.table_schema}.{table.table_name}", table_info['location'], table_info['partitionColumns'], table_info['clusteringColumns'], table_info['properties'], table_info['format']])
            createtab_stmt = spark.sql(f"SHOW CREATE TABLE {catalog_name}.{table.table_schema}.{table.table_name}").first()['createtab_stmt']
            tables_create_stmt.append([f"{catalog_name}.{table.table_schema}.{table.table_name}", createtab_stmt])
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            table_errors.append([f"{catalog_name}.{table.table_schema}.{table.table_name}", f"An unexpected error occurred: {e}"])

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

catalogs_df = spark.createDataFrame(catalogs, ['catalog'])
catalogs_df.write.format("delta").mode("overwrite").save(f"{storage_location}/uc_dr_catalogs")

if len(table_errors) > 0:
    table_errors_df = spark.createDataFrame(table_errors, ['table', 'error'])
    display(table_errors_df)
