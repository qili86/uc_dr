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
dbutils.widgets.text("storageLocation", "abfss://metadata@eastus2external.dfs.core.windows.net/", "Storage with source catalog info")
dbutils.widgets.text("rootExternalStorage", "abfss://data@starbucksdev.dfs.core.windows.net/daibo/", "Root of external tables' path")
dbutils.widgets.text("catalogName", "", "catalog name, it is empty by default to run all catalog, set it to a specific catalog for testing")

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
root_externalstorage =  dbutils.widgets.get("rootExternalStorage")
catalog_name = dbutils.widgets.get("catalogName")

print(storage_location)
print(root_externalstorage)
print(catalog_name)

# COMMAND ----------

# MAGIC %run ./utility/shared_functions

# COMMAND ----------

catalogs_df = spark.read.format("delta").load(f"{storage_location}/uc_dr_catalogs")
display(catalogs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create catalog/schema/external table

# COMMAND ----------

tables_details_df = spark.read.format("delta").load(f"{storage_location}/uc_dr_tables_details")

# COMMAND ----------

display(tables_details_df)

# COMMAND ----------

if catalog_name:
    catalogs_df = catalogs_df.filter(f"catalog = '{catalog_name}'")
display(catalogs_df)

# COMMAND ----------

tables_details_df = spark.read.format("delta").load(f"{storage_location}/uc_dr_tables_details")
table_errors = [["dummy_table", "dummy_error"]]
table_properities_system_level = ['delta.liquid.clusteringColumns', 'delta.rowTracking.materializedRowIdColumnName', 'delta.rowTracking.materializedRowCommitVersionColumnName']

#delete catalog which are not in primary any more
deleted_catalog_list = get_deleted_catalog(catalogs_df)
if len(deleted_catalog_list) > 0:
    print(f"----These catalogs are not exsiting in primary: {deleted_catalog_list}, and they will be deleted")
    for catalog in deleted_catalog_list:
        drop_schema_stm = f"DROP CATALOG IF EXISTS {catalog} CASCADE"
        print(drop_schema_stm)
        spark.sql(drop_schema_stm)

for catalog in catalogs_df.collect():
    catalog_name = catalog.catalog
    print(catalog_name)
    catalog_df = spark.read.format("delta").load(f"{storage_location}/{catalog_name}/catalogs") 
    for catalog in catalog_df.collect():     
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog.catalog_name} COMMENT '{catalog.comment}'")
        spark.sql(f"ALTER CATALOG {catalog.catalog_name} SET OWNER to `{catalog.catalog_owner}`")

    #Get only user schemas
    schemas_df = spark.read.format("delta").load(f"{storage_location}/{catalog_name}/schemata").filter("schema_name<>'information_schema'")

    #delete schemas which are not in primary any more
    deleted_schema_list = get_deleted_schema(catalog_name, schemas_df)
    if len(deleted_schema_list) > 0:
        print(f"----These schemas are not exsiting in primary: {deleted_schema_list}, and they will be deleted")
        for schema in deleted_schema_list:
            drop_schema_stm = f"DROP SCHEMA IF EXISTS {schema} CASCADE"
            print(drop_schema_stm)
            spark.sql(drop_schema_stm)
    
    #Create all user schemas on the target catalog

    for schema in schemas_df.collect():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema.schema_name} COMMENT '{schema.comment}'")
        spark.sql(f"ALTER SCHEMA {catalog_name}.{schema.schema_name} SET OWNER to `{schema.schema_owner}`")

    #Get only external user tables
    tables_df = spark.read.format("delta").load(f"{storage_location}/{catalog_name}/tables").filter("table_schema<>'information_schema' and table_type='EXTERNAL'")
    tables_df = tables_df.withColumn("full_name", concat_ws(".", col("table_catalog"), col("table_schema"), col("table_name")))
    tables_df = tables_df.join(tables_details_df, on="full_name", how="left")
    
    deleted_table_list = get_deleted_table(catalog_name, tables_df)
    if len(deleted_table_list) > 0:
        print(f"----These tables are not exsiting in primary: {deleted_table_list}, and they will be deleted")
        for table in deleted_table_list:
            drop_table_stm = f"DROP TABLE IF EXISTS {table}"
            print(drop_table_stm)
            spark.sql(drop_table_stm)
    
    for table in tables_df.collect():
        columns_df = spark.read.format("delta").load(f"{storage_location}/{catalog_name}/columns").filter((col("table_schema") == table.table_schema) & (col("table_name") == table.table_name))
        print(f"{catalog_name}.{table.table_schema}.{table.table_name}")
        columns = return_schema(columns_df)
        #Create Table
        try:
            drop_table_stm = f"DROP TABLE IF EXISTS {catalog_name}.{table.table_schema}.{table.table_name}"
            print(drop_table_stm)
            spark.sql(drop_table_stm)
            
            ct_st = f"CREATE TABLE IF NOT EXISTS {catalog_name}.{table.table_schema}.{table.table_name}({columns}) USING {table.format} COMMENT '{table.comment}' LOCATION '{table.location}'"
            if table.partitionColumns is not None and table.partitionColumns !=[]:
                pks = ",".join(table.partitionColumns)
                ct_st = ct_st + f" PARTITIONED BY ({pks})" 
            if table.clusteringColumns is not None and table.clusteringColumns != []:
                cks = ",".join(table.clusteringColumns)
                ct_st = ct_st + f" CLUSTER BY ({cks})" 
            if table.properties is not None and table.properties != {}:
                ppts=table.properties
                for ppt in table_properities_system_level:
                    ppts.pop(ppt, None)
                if ppts != {}:
                    properties_str = "" 
                    for key, value in ppts.items():
                        properties_str=properties_str+ f"'{key}'='{value}',"
                    ct_st = ct_st + f" TBLPROPERTIES ({properties_str[:-1]})"
            print(ct_st)
            spark.sql(ct_st)
            spark.sql(f"ALTER TABLE {catalog_name}.{table.table_schema}.{table.table_name} SET OWNER to `{table.table_owner}`")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            table_errors.append([f"{catalog_name}.{table.table_schema}.{table.table_name}", f"An unexpected error occurred: {e}"])

table_errors_df = spark.createDataFrame(table_errors, ['table', 'error'])
if table_errors_df.count() > 1:
   display(table_errors_df)

# COMMAND ----------

# spark.sql(f"CREATE CATALOG IF NOT EXISTS uc_dr_dummy")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS uc_dr.dummy_test_cases")
# spark.sql(f"create table if not exists uc_dr.test_cases.delta_table1 (c1 int, c2 string) using delta location '{root_externalstorage}uc_dr/test_cases/delta_table1'")
# spark.sql(f"create table if not exists uc_dr.default.delta_table1 (c1 int, c2 string) using delta location '{root_externalstorage}uc_dr/default/delta_table1'")
