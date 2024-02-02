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

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
root_externalstorage =  dbutils.widgets.get("rootExternalStorage")

print(storage_location)
print(root_externalstorage)

#table_list = dbutils.fs.ls(storage_location)

# COMMAND ----------

import re
from pyspark.sql.functions import col, when, collect_list, upper,concat_ws,col

def return_schema(df):
    column_names = df.orderBy(df.ordinal_position.asc()).select("column_name", upper("full_data_type")).collect()
    schema = ""
    for x,y in column_names:
        sql =f''' {x} {y},'''
        schema += sql
        if y == []:
            break

    p = re.compile('(,$)')
    schema_no_ending_comma = p.sub('', schema)
    return(schema_no_ending_comma)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create catalog/schema/external table

# COMMAND ----------

tables_details_df = spark.read.format("delta").load(f"{storage_location}/uc_dr_tables_details")

# COMMAND ----------

display(tables_details_df)

# COMMAND ----------

catalogs_df = spark.read.format("delta").load(f"{storage_location}/uc_dr_catalogs")
tables_details_df = spark.read.format("delta").load(f"{storage_location}/uc_dr_tables_details")
table_errors = [["dummy_table", "dummy_error"]]
table_properities_system_level = ['delta.liquid.clusteringColumns', 'delta.rowTracking.materializedRowIdColumnName', 'delta.rowTracking.materializedRowCommitVersionColumnName']


for catalog in catalogs_df.collect():
    catalog_name = catalog.catalog
    print(catalog_name)
    catalog_df = spark.read.format("delta").load(f"{storage_location}/{catalog_name}/catalogs")
    
    for catalog in catalog_df.collect():     
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog.catalog_name} COMMENT '{catalog.comment}'")
        spark.sql(f"ALTER CATALOG {catalog.catalog_name} SET OWNER to `{catalog.catalog_owner}`")

    #Get only user schemas
    schemas_df = spark.read.format("delta").load(f"{storage_location}/{catalog_name}/schemata").filter("schema_name<>'information_schema'")

    #Drop the default schema
    spark.sql(f"DROP SCHEMA {catalog_name}.default CASCADE")

    #Create all user schemas on the target catalog
    for schema in schemas_df.collect():    
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema.schema_name} COMMENT '{schema.comment}'")
        spark.sql(f"ALTER SCHEMA {catalog_name}.{schema.schema_name} SET OWNER to `{schema.schema_owner}`")

    #Get only external user tables
    tables_df = spark.read.format("delta").load(f"{storage_location}/{catalog_name}/tables").filter("table_schema<>'information_schema' and table_type='EXTERNAL'")
    tables_df = tables_df.withColumn("full_name", concat_ws(".", col("table_catalog"), col("table_schema"), col("table_name")))
    tables_df = tables_df.join(tables_details_df, on="full_name", how="left")

    for table in tables_df.collect():
        columns_df = spark.read.format("delta").load(f"{storage_location}/{catalog_name}/columns").filter((col("table_schema") == table.table_schema) & (col("table_name") == table.table_name))
        print(f"{catalog_name}.{table.table_schema}.{table.table_name}")
        columns = return_schema(columns_df)
        #Create Table
        # spark.sql(f"CREATE OR REPLACE TABLE {catalog_name}.{table.table_schema}.{table.table_name}({columns}) COMMENT '{table.comment}' LOCATION '{root_externalstorage}{table.table_schema}/{table.table_name}'")
        try:
            ct_st = f"CREATE OR REPLACE TABLE {catalog_name}.{table.table_schema}.{table.table_name}({columns}) COMMENT '{table.comment}' LOCATION '{table.location}'"
            if table.partitionColumns != []:
                pks = ",".join(table.partitionColumns)
                ct_st = ct_st + f" PARTITIONED BY ({pks})" 
            if table.clusteringColumns != []:
                cks = ",".join(table.clusteringColumns)
                ct_st = ct_st + f" CLUSTER BY ({cks})" 
            if table.properties != {}:
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
