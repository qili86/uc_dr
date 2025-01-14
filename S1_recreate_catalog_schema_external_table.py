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

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "abfss://system@eastus2external.dfs.core.windows.net/", "Storage with source catalog info")

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
print(storage_location)

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create catalog

# COMMAND ----------

catalogs_df = spark.read.format("delta").load(f"{storage_location}/catalogs").filter("catalog_owner<>'System user'")
# delete catalog which are not in primary any more
deleted_catalog_list = get_deleted_catalog(catalogs_df)
if len(deleted_catalog_list) > 0:
    print(f"----These catalogs are not exsiting in primary: {deleted_catalog_list}, and they will be deleted")
    for catalog in deleted_catalog_list:
        drop_catalog_stm = f"DROP CATALOG IF EXISTS {catalog} CASCADE"
        print(drop_catalog_stm)
        spark.sql(drop_catalog_stm)

for catalog in catalogs_df.collect():     
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog.catalog_name} COMMENT '{catalog.comment}'")
    spark.sql(f"ALTER CATALOG {catalog.catalog_name} SET OWNER to `{catalog.catalog_owner}`") 
    """
    Docs: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-catalog.html
    CREATE CATALOG [ IF NOT EXISTS ] catalog_name
        [ USING SHARE provider_name . share_name ]
        [ MANAGED LOCATION 'location_path' ]
        [ COMMENT comment ]

    CREATE FOREIGN CATALOG [ IF NOT EXISTS ] catalog_name
        USING CONNECTION connection_name
        [ COMMENT comment ]
        OPTIONS ( { option_name = option_value } [ , ... ] )

    Support Notes:
        * Need support for MANAGED LOCATION
        * Might need support for Foreign catalogs
        * Might need support for Using Share provider_name.share_name
    """

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create schema

# COMMAND ----------

    #Get only user schemas
schemas_details_df = spark.read.format("delta").load(f"{storage_location}/uc_dr_schemas_details")
schemas_df = spark.read.format("delta").load(f"{storage_location}/schemata").filter(f"schema_name<>'information_schema' and schema_owner <> 'System user'")
schemas_df = schemas_df.withColumn("name", concat_ws(".", col("catalog_name"), col("schema_name")))
schemas_df = schemas_df.join(schemas_details_df, on="name", how="left")

deleted_schema_list = get_deleted_schema(schemas_df)
if len(deleted_schema_list) > 0:
  print(f"----These tables are not exsiting in primary: {deleted_schema_list}, and they will be deleted")
  for schema in deleted_schema_list:
   drop_schema_stmt = f"DROP SCHEMA IF EXISTS {schema} CASCADE"
   print(drop_schema_stmt)
   spark.sql(drop_schema_stmt)

# COMMAND ----------

"""
    Docs: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-schema.html
    CREATE SCHEMA [ IF NOT EXISTS ] schema_name
        [ COMMENT schema_comment ]
        [ LOCATION schema_directory | MANAGED LOCATION location_path ]
        [ WITH DBPROPERTIES ( { property_name = property_value } [ , ... ] ) ]
"""
for schema in schemas_df.collect():
    cs_stmt = f"CREATE SCHEMA IF NOT EXISTS {schema.catalog_name}.{schema.schema_name} COMMENT '{schema.comment}'"
    if schema.location is not None and schema.location != "":
        cs_stmt = cs_stmt + f" MANAGED LOCATION '{schema.location}'"
    if schema.properties is not None and schema.properties != "":
        properties_str = ""
        properties_dict = get_schema_properties(schema.properties)
        if properties_dict != {}:
            properties_str = "" 
            for key, value in properties_dict.items():
                properties_str=properties_str+ f"'{key}'='{value}',"
        cs_stmt = cs_stmt + f" WITH DBPROPERTIES ({properties_str[:-1]})"
    print(cs_stmt)
    spark.sql(cs_stmt)
    spark.sql(f"ALTER SCHEMA {schema.catalog_name}.{schema.schema_name} SET OWNER to `{schema.schema_owner}`")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create external table

# COMMAND ----------

tables_create_stmts_df = spark.read.format("delta").load(f"{storage_location}/uc_dr_tables_create_stmts")
tcs_dict = {}
for tcs in tables_create_stmts_df.collect():
    tcs_dict[tcs['name']] = tcs['stmt']
print(tcs_dict)

# COMMAND ----------

table_errors = []
    
#Get only external user tables
tables_df = spark.read.format("delta").load(f"{storage_location}/tables").filter(f"table_schema<>'information_schema' and table_type='EXTERNAL'")
tables_df = tables_df.withColumn("full_name", concat_ws(".", col("table_catalog"), col("table_schema"), col("table_name")))
tables_df = tables_df.join(tables_details_df, on="full_name", how="left")

deleted_table_list = get_deleted_table(tables_df)
if len(deleted_table_list) > 0:
    print(f"----These tables are not exsiting in primary: {deleted_table_list}, and they will be deleted")
    for table in deleted_table_list:
        drop_table_stmt = f"DROP TABLE IF EXISTS {table}"
        print(drop_table_stmt)
        spark.sql(drop_table_stmt)

for table in tables_df.collect():
    name = f"{table.table_catalog}.{table.table_schema}.{table.table_name}"
    print(name)
    """
    Docs: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html
    for delta table, using CREATE OR REPLACE
    for none-delta table, using CREATE IF NOT EXISTS 
    NOT Supported for Schema Change (drop/rename columns), otherwise, we have to use drop and recreate
    """
    try:
        ct_stmt = tcs_dict[name]
        print(f"original ct_stmt: {ct_stmt}")
        ct_stmt = process_ct_stmt(ct_stmt)
        print(f"final ct_stmt: {ct_stmt}")
        spark.sql(ct_stmt)
        spark.sql(f"ALTER TABLE {name} SET OWNER to `{table.table_owner}`")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        table_errors.append([name, f"An unexpected error occurred: {e}"])

if len(table_errors) > 0:
    table_errors_df = spark.createDataFrame(table_errors, ['table', 'error'])
    display(table_errors_df)

# COMMAND ----------

# spark.sql(f"CREATE CATALOG IF NOT EXISTS uc_dr_dummy")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS uc_dr.dummy_test_cases")
# spark.sql(f"create table if not exists uc_dr.test_cases.delta_table1 (c1 int, c2 string) using delta location 'abfss://data@starbucksdev.dfs.core.windows.net/daibo/uc_dr/test_cases/delta_table1'")
# spark.sql(f"create table if not exists uc_dr.default.delta_table1 (c1 int, c2 string) using delta location 'abfss://data@starbucksdev.dfs.core.windows.net/daibo/uc_dr/default/delta_table1'")
