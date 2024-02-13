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

catalogs_df = spark.read.format("delta").load(f"{storage_location}/catalogs").filter("catalog_owner<>'System user'")
display(catalogs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create catalog/schema/external table

# COMMAND ----------

tables_details_df = spark.read.format("delta").load(f"{storage_location}/uc_dr_tables_details")
display(tables_details_df)
tables_create_stmts_df = spark.read.format("delta").load(f"{storage_location}/uc_dr_tables_create_stmts")
tcs_dict = {}
for tcs in tables_create_stmts_df.collect():
    tcs_dict[tcs['name']] = tcs['stmt']
print(tcs_dict)

# COMMAND ----------

schemas_details_df = spark.read.format("delta").load(f"{storage_location}/uc_dr_schemas_details")
display(schemas_details_df)

# COMMAND ----------

table_errors = []
table_properities_system_level = ['delta.liquid.clusteringColumns', 'delta.rowTracking.materializedRowIdColumnName', 'delta.rowTracking.materializedRowCommitVersionColumnName']

# delete catalog which are not in primary any more
deleted_catalog_list = get_deleted_catalog(catalogs_df)
if len(deleted_catalog_list) > 0:
    print(f"----These catalogs are not exsiting in primary: {deleted_catalog_list}, and they will be deleted")
    for catalog in deleted_catalog_list:
        drop_schema_stm = f"DROP CATALOG IF EXISTS {catalog} CASCADE"
        print(drop_schema_stm)
        spark.sql(drop_schema_stm)

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

    #Get only user schemas
    schemas_df = spark.read.format("delta").load(f"{storage_location}/schemata").filter(f"schema_name<>'information_schema' and catalog_name='{catalog.catalog_name}'")
    schemas_df = schemas_df.withColumn("name", concat_ws(".", col("catalog_name"), col("schema_name")))
    schemas_df = schemas_df.join(schemas_details_df, on="name", how="left")
    display(schemas_df)

    # delete schemas which are not in primary any more
    deleted_schema_list = get_deleted_schema(catalog.catalog_name, schemas_df)
    if len(deleted_schema_list) > 0:
        print(f"----These schemas are not exsiting in primary: {deleted_schema_list}, and they will be deleted")
        for schema in deleted_schema_list:
            drop_schema_stmt = f"DROP SCHEMA IF EXISTS {schema} CASCADE"
            print(drop_schema_stmt)
            spark.sql(drop_schema_stmt)
    
    #Create all user schemas on the target catalog
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

    #Get only external user tables
    tables_df = spark.read.format("delta").load(f"{storage_location}/tables").filter(f"table_schema<>'information_schema' and table_type='EXTERNAL' and table_catalog='{catalog.catalog_name}'")
    tables_df = tables_df.withColumn("full_name", concat_ws(".", col("table_catalog"), col("table_schema"), col("table_name")))
    tables_df = tables_df.join(tables_details_df, on="full_name", how="left")
    

    deleted_table_list = get_deleted_table(catalog.catalog_name, tables_df)
    if len(deleted_table_list) > 0:
        print(f"----These tables are not exsiting in primary: {deleted_table_list}, and they will be deleted")
        for table in deleted_table_list:
            drop_table_stmt = f"DROP TABLE IF EXISTS {table}"
            print(drop_table_stmt)
            spark.sql(drop_table_stmt)
    
    for table in tables_df.collect():
        name = f"{table.table_catalog}.{table.table_schema}.{table.table_name}"
        print(name)
        columns_df = spark.read.format("delta").load(f"{storage_location}/columns").filter((col("table_catalog") == table.table_catalog) & (col("table_schema") == table.table_schema) & (col("table_name") == table.table_name))
        columns = return_schema(columns_df)
        #Create Table

        """
        Docs: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html
        
        Support Notes:
            * The current solution is creating ddl from scratch, and is not covering all cases
            * Ideally could use "show table create <table_name>", but it has to remove some table properities to make it work
        """

        try:
            drop_table_stmt = f"DROP TABLE IF EXISTS {name}"
            print(drop_table_stmt)
            spark.sql(drop_table_stmt)
            """
            Support Notes:
                * The current solution is drop table and recreate table in case table schema changes e.g. drop/rename columns, otherwise
                it is better to use CREATE OR REPLACE TABLE ... because drop table will drop delta histories 
                Moreover CREATE OR REPLACE TABLE might not support for parquet, csv format, please test out those cases,if so, for such types, you might
                still drop and recreate, and they basically don't have history either
            """
            ct_stmt = tcs_dict[name]
            # ct_stmt = f"CREATE TABLE IF NOT EXISTS {table.table_catalog}.{table.table_schema}.{table.table_name}({columns}) USING {table.format} COMMENT '{table.comment}' LOCATION '{table.location}'"
            # if table.partitionColumns is not None and table.partitionColumns !=[]:
            #     pks = ",".join(table.partitionColumns)
            #     ct_stmt = ct_stmt + f" PARTITIONED BY ({pks})" 
            # if table.clusteringColumns is not None and table.clusteringColumns != []:
            #     cks = ",".join(table.clusteringColumns)
            #     ct_stmt = ct_stmt + f" CLUSTER BY ({cks})" 
            # if table.properties is not None and table.properties != {}:
            #     ppts=table.properties
            #     for ppt in table_properities_system_level:
            #         ppts.pop(ppt, None)
            #     if ppts != {}:
            #         properties_str = "" 
            #         for key, value in ppts.items():
            #             properties_str=properties_str+ f"'{key}'='{value}',"
            #         ct_stmt = ct_stmt + f" TBLPROPERTIES ({properties_str[:-1]})"
            print(f"before------- {ct_stmt}")
            modified_ct_stmt = process_ct_stmt(ct_stmt)
            print(f"after------- {modified_ct_stmt}")
            spark.sql(modified_ct_stmt)
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

# COMMAND ----------

def process_ctstmt(ct_stmt)
    pattern = re.compile(r"TBLPROPERTIES\s*\([^)]+\)")
    modified_ct_stmt = re.sub(pattern, "", ct_stmt)
    return modified_ct_stmt
