# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import logging
from collections.abc import Iterable
import concurrent
from delta import *

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storage_location", "/tmp/testing_sb_dr/dr1")
dbutils.widgets.text("import_type", "dr")
storage_location = dbutils.widgets.get("storage_location")
import_type = dbutils.widgets.get("import_type")
print(storage_location)
print(import_type)

# COMMAND ----------

class CommandExecutor:
    """
        A class to run spark commands in parallel.
    """

    def __init__(self, spark):
        self.spark = spark

    def _spark_command_execute(self, command) -> DataFrame:
        #return self.spark.sql(command)
        print("Command: ", command)
        return None

    def _execute_and_collect(self, _iterable: Iterable) -> list[any]:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            logging.info(f"Default number of threads used: {executor._max_workers}")
            futures = {executor.submit(self._spark_command_execute, x): x for x in _iterable}
            return [future.result() for future in concurrent.futures.as_completed(futures)]

    def parallelize_method_over_iterable(self, _iterable: Iterable, max_batch_size: int = 500):
        """
                Parallelizes the execution of a callable over each element in an iterable.

                Batching of the processing will occur if the length of the iterable is greater than
                the max_batch size, which is to prevent overloading the driver queue.

                Args:
                    _iterable: An iterable collection of elements.
                    max_batch_size: the maximum concurrent submissions sent to the driver to be processed.
                        If -1, this value will be ignored.

                Returns:
                    A list of tuples with each input elements and its corresponding result or error message.

                Note:
                    This function assumes an existing Spark session `spark` and requires the `threading` module.
          """
        if max_batch_size < 0 or len(_iterable) <= max_batch_size:
            logging.info("We are not batching the parallelize_method_over_iterable request.")
            return self._execute_and_collect(_iterable)
        else:
            logging.info("We are batching the parallelize_method_over_iterable request.")
            output = []
            for i in range(0, len(_iterable), max_batch_size):
                batch = _iterable[i:i + max_batch_size]
                output.extend(self._execute_and_collect(batch))
            return output

# COMMAND ----------


class TablesImporter:
    """
        A class to import the managed tables.
    """
    def __init__(self, spark, storage_location, import_type='dr'):
        self.spark = spark
        self.import_type = import_type
        self.storage_location = storage_location
        self.data_storage_path = self.storage_location + "/" + "managed_tables_data"
        self.meta_mt_storage_path = self.storage_location + "/" + "tables_definitions/managed_tables"
        self.meta_et_storage_path = self.storage_location + "/" + "tables_definitions/external_tables"
        self.privileges_storage_path = self.storage_location + "/" + "tables_priviliges"
        self.commandExecutor = CommandExecutor(self.spark)

    def __get_new_catalogs_and_schemas(self, managed_tables_primary: DataFrame,  external_tables_primary: DataFrame) -> DataFrame:
        tables_primary = managed_tables_primary.select("table_catalog", "table_schema", "table_name").union(external_tables_primary.select("table_catalog", "table_schema", "table_name"))
        # Find the catalogs & schemas that are needed to be created first.
        create_catalog_stmt_template = """CREATE CATALOG IF NOT EXISTS `<catalog_name>`"""
        create_schema_stmt_template = """CREATE SCHEMA IF NOT EXISTS `<catalog_name>`.`<schema_name>`"""   
        tables_secondary = self.spark.read.format("delta").table("system.information_schema.tables")
        
        new_catalogs_schemas = (tables_primary.alias("p").join(tables_secondary.alias("s"), (tables_primary.table_catalog == tables_secondary.table_catalog) & (tables_primary.table_schema == tables_secondary.table_schema), "leftanti").select("p.table_catalog", "p.table_schema").withColumn("create_catalog_stmt_template", lit(create_catalog_stmt_template)).withColumn("create_schema_stmt_template", lit(create_schema_stmt_template)).withColumn("create_catalog_stmt", replace(col("create_catalog_stmt_template"), lit("<catalog_name>"), col("table_catalog"))).withColumn("create_schema_stmt", replace(replace(col("create_schema_stmt_template"), lit("<catalog_name>"), col("table_catalog")), lit("<schema_name>"), col("table_schema"))))
        return new_catalogs_schemas.select("create_catalog_stmt", "create_schema_stmt")
    
    def __create_catalogs_and_schemas(self, managed_tables: DataFrame, external_tables: DataFrame):
        new_catalogs_and_schemas = self.__get_new_catalogs_and_schemas(managed_tables, external_tables).collect()
        if not new_catalogs_and_schemas:
            return
        catalog_stmts = []
        schema_stmts = []
        for row in new_catalogs_and_schemas:
            catalog_stmts.append(row["create_catalog_stmt"])
            schema_stmts.append(row["create_schema_stmt"])
        if catalog_stmts:
          print("creating catalogs", catalog_stmts)
          self.commandExecutor.parallelize_method_over_iterable(catalog_stmts)
        if schema_stmts:
          print("creating schemas", schema_stmts)
          self.commandExecutor.parallelize_method_over_iterable(schema_stmts)

    def __clone_managed_tables(self, managed_tables: DataFrame):
        deep_clone_statement_template = """CREATE OR REPLACE TABLE <table_name> DEEP CLONE delta.`<source_location>` LOCATION '<target_location>'"""
        managed_tables = managed_tables.withColumn("deep_clone_statement_template", lit(deep_clone_statement_template))
        deep_clone_statements = []
        if self.import_type.lower() == 'failback':
            deep_clone_statements = (managed_tables.withColumn("deep_clone_statement", replace(replace(replace(col("deep_clone_statement_template"), lit("<table_name>"), col("key")), lit("<source_location>"), col("secondary_storage_location")), lit("<target_location>"), col("primary_storage_location"))).select("deep_clone_statement")).collect()
        else:
            deep_clone_statements = (managed_tables.withColumn("deep_clone_statement", replace(replace(replace(col("deep_clone_statement_template"), lit("<table_name>"), col("key")), lit("<source_location>"), col("primary_storage_location")), lit("<target_location>"), col("secondary_storage_location"))).select("deep_clone_statement")).collect()
        statements = []
        for deepclonestmt in deep_clone_statements:
            statements.append(deepclonestmt)
        if statements:
          self.commandExecutor.parallelize_method_over_iterable(statements)

    def __create_external_tables(self, external_tables):
        tables = self.spark.read.format("delta").load(self.meta_et_storage_path).collect()
        if tables:
            table_statements = []
            for table in tables:
              table_statements.append(table['createtab_stmt'])
            self.commandExecutor.parallelize_method_over_iterable(table_statements)

    def __grant_permissions(self):
            primary = self.spark.read.format("delta").load(self.privileges_storage_path)
            secondary = self.spark.read.format("delta").table("`system`.information_schema.table_privileges")
            privileges = (primary.join(secondary, (primary.grantor == secondary.grantor) & (primary.grantee == secondary.grantee) & (primary.table_catalog == secondary.table_catalog)
                          & (primary.table_schema == secondary.table_schema)  & (primary.table_name == secondary.table_name) & (primary.privilege_type == secondary.privilege_type), "leftanti"))
            grant_stmt_template = """ GRANT <privilege_types> ON TABLE TO <principal>"""
            grant_stmts = privileges.withColumn("grant_stmt_template", lit(grant_stmt_template)).withColumn("grant_stmt", replace(replace(col("grant_stmt_template"), lit("<privilege_types>"), col("privilege_type")), lit("<principal>"), col("grantee"))).select("grant_stmt").collect()
            statements = []
            for stmt in grant_stmts:
                statements.append(stmt)
            if statements:
              self.commandExecutor.parallelize_method_over_iterable(statements)

    def import_tables(self):
        managed_tables = self.spark.read.format("delta").load(self.meta_mt_storage_path)
        external_tables = self.spark.read.format("delta").load(self.meta_et_storage_path)
        table_privileges = self.spark.read.format("delta").load(self.privileges_storage_path)
        # create catalogs and schema if not exist 
        self.__create_catalogs_and_schemas(managed_tables, external_tables)
        # deep clone managed tables
        self.__clone_managed_tables(managed_tables)
        # create external tables
        self.__create_external_tables(external_tables)
        # grant the permissions
        self.__grant_permissions()

# COMMAND ----------

importer= TablesImporter(spark, storage_location)
importer.import_tables()

# COMMAND ----------


