# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This notebook contains a sample code for migrating tables (tested with managed, should work with unmanaged as well) between two UC metastores. 
# MAGIC
# MAGIC # How it works
# MAGIC
# MAGIC This notebook uses the [Delta Deep Clone](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-clone) operation to replicate data between two tables with transactionality guarantees.  Delta Deep Clone operation is also incremental, so it will copy only changes that are done on the table since previous deep clone operation.
# MAGIC
# MAGIC In Unity Catalog managed tables are stored in the locations that could be different between two UC metastores, and to solve that problem we need to use an intermediate location.  The workflow looks as following:
# MAGIC
# MAGIC 1. The Databricks job in the primary region is cloning tables to the intermediate storage account and registering them in the dedicated UC catalog (for reference)
# MAGIC 1. The Databricks job in the secondary (failover region) is iterating over the configured tables and cloning the data from the intermediate storage into UC Metastore in the secondary region, if necessary creating catalogs & schema (although it could be better if at least catalog is pre-created)
# MAGIC
# MAGIC ```
# MAGIC  ┌────────────────┐                              ┌────────────────┐
# MAGIC  │                │                              │                │
# MAGIC  │ UC Metastore 1 │                              │ UC Metastore 2 │
# MAGIC  │                │                              │                │
# MAGIC  └──────┬─────────┘                              └────────▲───────┘
# MAGIC         │                                                 │
# MAGIC         │                                                 │
# MAGIC         │                                                 │
# MAGIC ┌───────▼───────────┐                          ┌──────────┴──────────┐
# MAGIC │                   │                          │                     │
# MAGIC │ Primary workspace │                          │ Secondary workspace │
# MAGIC │                   │                          │                     │
# MAGIC └───────────┬───────┘                          └──────────▲──────────┘
# MAGIC             │                                             │
# MAGIC             │Push                                   Pull  │
# MAGIC             │         ┌──────────────┐                    │
# MAGIC             │         │              │                    │
# MAGIC             └────────►│ Intermediate ├────────────────────┘
# MAGIC                       │   storage    │
# MAGIC                       └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC *Biggest drawback right now is that it requires an intermediate storage for cloning of managed tables.* See Notes below for some details. 
# MAGIC
# MAGIC
# MAGIC # How to use
# MAGIC
# MAGIC The notebook requires some parameters to enter either via widgest when executing interactively, or they could be specified as notebooks parameters when executing as jobs (name of the parameter is specified in the list):
# MAGIC
# MAGIC * `region` - DR region type - possible values are: `Primary` for source region, and `Secondary` for failover region.
# MAGIC * `cred_name` - name of UC storage credential that will be used to access intermediate storage location.  This storage credential should be already created in both metastores.
# MAGIC * `ext_location_url` - `abfss` URL of the intermediate storage account. It's recommended to put that storage account in the failover/secondary region so we can perform deep clone even if the primary region goes down completely
# MAGIC * `ext_location_name` name that will be used for UC external location for intermediate storage. If external location isn't registered, then it will be created.
# MAGIC * `clone_catalog` - the name UC catalog that will be used for registration of the cloned tables.
# MAGIC
# MAGIC We also need to configure what catalogs/schemas/tables should be replicated. Right now it's defined by the `to_replicate` variable in the next notebook cell (although it could be taken from some source, see Notes below). This variable should be a Python dictionary consisting of UC catalog name as a key, and value should be another Python dictionary where key is the schema name inside that UC catalog, and value is a list of tables to replicate.
# MAGIC
# MAGIC ```
# MAGIC to_replicate = {
# MAGIC   "dr_catalog": { # catalog name
# MAGIC     "dr_schema": ["table1", "table2"], # mapping from schema name (`dr_schema`) to a list of tables to clone
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Code should be executed on the UC-enabled cluster, and user who execute the code should have permissions to read tables and create necessary objects.
# MAGIC
# MAGIC ## Additional notes
# MAGIC
# MAGIC * This cloning right now is single threaded, but performance could be improved by multi-threaded execution as it's mostly file copy operations.  We only need to pre-create catalogs and schemas to avoid clashes.
# MAGIC * We can list catalogs/schemas/tables & select what to replicate based on the tags assigned to catalogs/schemas/tables - we should be able to get that from `information_schema`
# MAGIC * We can get from information schema also the information if the table is managed/unmanaged and then do intermediate clone only for managed tables, and work with unmanaged directly
# MAGIC * This should run on UC-enabled cluster and under identity that has necessary permissions
# MAGIC * Theoretically we can use last updated timestamp from the `information_schema` or similar table and use it for decision if we should replicate it or not.
# MAGIC * Theoretically it's possible to avoid cloning of data into intermediate storage, but this will require registration of temporary tables (in each metastore) pointing to managed tables and use that for cloning. But that will require generation of some config file or something like for each metastore as we can't read the metastore directly (or can we via REST API?)

# COMMAND ----------

# This could be either done explicitly, or we can list catalogs/schemas/tables & select what to replicate based on the tags.
# The structure is following:
#  - catalog -> dict of schemas -> list of tables
to_replicate = {
  "dr_catalog": { # catalog name
    "dr_schema": ["table1", "table2"], # mapping from schema name (`dr_schema`) to a list of tables to clone
    # "dest_catalog": "aott_dest"  # optional name of the target catalog (if it's different from the source)
  }
}
# Python set to control what parameters shouldn't be treated as schema names
param_names = {"dest_catalog"}

# COMMAND ----------

dbutils.widgets.dropdown("region", "Primary", ["Primary", "Secondary"], "DR Region")
dbutils.widgets.text("cred_name", "", "UC Storage credentials name")
dbutils.widgets.text("ext_location_name", "clones", "Name for clone external storage location")
dbutils.widgets.text("ext_location_url", "abfss://<container>@<storage>.dfs.core.windows.net", "URL for clone storage location")
dbutils.widgets.text("clone_catalog", "clones", "Name of the UC catalog for cloned tables")

# COMMAND ----------

is_primary_region = dbutils.widgets.get("region") == "Primary"
cred_name = dbutils.widgets.get("cred_name")
ext_location_name = dbutils.widgets.get("ext_location_name")
ext_location_url = dbutils.widgets.get("ext_location_url").strip("/") + "/"
clone_catalog = dbutils.widgets.get("clone_catalog")

# COMMAND ----------

spark.sql(f"CREATE EXTERNAL LOCATION IF NOT EXISTS {ext_location_name} URL '{ext_location_url}' WITH ( STORAGE CREDENTIAL `{cred_name}` )")

# COMMAND ----------

csql = f"CREATE CATALOG IF NOT EXISTS {clone_catalog} MANAGED LOCATION '{ext_location_url}'"
spark.sql(csql)

# COMMAND ----------

import re
name_cleanup_re = re.compile(r"[\W-]")

def gen_schema_name(catalog: str, schema: str):
  """Generates a schema name inside the clones catalog by concatenating src catalog + schema
  """
  cat_name = re.sub(name_cleanup_re, "_", catalog)
  schema_name = re.sub(name_cleanup_re, "_", schema)
  return f"{cat_name}_{schema_name}"

def gen_table_dir(catalog: str, schema: str, table: str):
  """Generates directory name for a cloned table
  """
  table_name = re.sub(name_cleanup_re, "_", table)
  return f"{gen_schema_name(catalog, schema)}/{table_name}/"

# COMMAND ----------

# just for emulation inside the same metastore
# is_primary_region = True
# is_primary_region = False

# COMMAND ----------

# Main cycle for cloning
for catalog, scm in to_replicate.items():
  dest_catalog = scm.get("dest_catalog", catalog)
  if not is_primary_region:
    # Really we need to pre-create catalogs via terraform or something like that to simplify specification of locations 
    spark.sql(f"create catalog if not exists `{dest_catalog}`")
  for schema, tables in scm.items():
    if schema in param_names:
      continue
    schema_name = gen_schema_name(catalog, schema)
    if not is_primary_region:
      # We may need to pre-create schemas that to simplify specification of locations, or find a way how to specify them in the target region
      spark.sql(f"create schema if not exists `{dest_catalog}`.{schema}")
      spark.sql(f"create schema if not exists `{clone_catalog}`.{schema_name}")
    else:
      # Create schema in the clones catalog
      spark.sql(f"create schema if not exists `{clone_catalog}`.{schema_name}")
    for table in tables:
      print(f"Processing {catalog}.{schema}.{table}")
      # TODO: we'll need to gracefully handle if table doesn't exist - it would be easier if we take tables to clone from the information_schema of catalogs
      table_dir = f"{ext_location_url}{gen_table_dir(catalog, schema, table)}"
      if is_primary_region:
        # clone the table into the clone external location
        sql = f"""create or replace table `{clone_catalog}`.`{schema_name}`.`{table}`
        deep clone `{catalog}`.`{schema}`.`{table}` location '{table_dir}'"""
        print("Executing", sql)
        spark.sql(sql)
      else:
        sql = f"""create table if not exists `{clone_catalog}`.`{schema_name}`.`{table}`
        location '{table_dir}'"""
        print("Executing", sql)
        spark.sql(sql)
        sql = f"""create or replace table `{dest_catalog}`.`{schema}`.`{table}`
        deep clone `{clone_catalog}`.`{schema_name}`.`{table}`"""
        print("Executing", sql)
        spark.sql(sql)
