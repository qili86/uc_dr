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

storage_location = dbutils.widgets.get("storage_location")

print(storage_location)

# COMMAND ----------

class CommandExecutor:
    """
        A class to run spark commands in parallel.
    """

    def __init__(self, spark):
        self.spark = spark

    def _spark_command_execute(self, key, command) -> DataFrame:
      try:
        return self.spark.sql(command).withColumn("key", lit(key))
      except Exception as e:
        print("Error while excuting the command! ", command)
      return None

    def _execute_and_collect(self, _iterable: Iterable) -> list[any]:
        results = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            logging.info(f"Default number of threads used: {executor._max_workers}")
            futures = {executor.submit(self._spark_command_execute, x[0], x[1]): x for x in _iterable}
            for future in concurrent.futures.as_completed(futures):
              result = future.result()
              if result:
                results.append(result)
        return results

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

class TablesExporter:
    """
        A class to export the managed & external table definitions.
    """
    def __init__(self, spark, storage_location):
        self.spark = spark
        self.storage_location = storage_location
        self.data_storage_path = self.storage_location + "/" + "managed_tables_data"
        self.meta_storage_path = self.storage_location + "/" + "tables_definitions"
        self.privileges_storage_path = self.storage_location + "/" + "tables_priviliges"
        self.commandExecutor = CommandExecutor(self.spark)

    def __get_managed_tables(self) -> DataFrame:
        managed_tables_df = (self.spark.read.format("delta").table("system.information_schema.tables").filter(
            f"table_schema<>'information_schema' and table_type='MANAGED' and table_owner <> 'System user' and table_catalog <> '__databricks_internal' and table_catalog = 'main' and data_source_format = 'DELTA'")
                             .withColumn("key", concat(lit("`"), col("table_catalog"), lit("`.`"), col("table_schema"), lit("`.`"), col("table_name"), lit("`")))
                             .withColumn("secondary_storage_location", concat(lit(self.data_storage_path), lit("/"), col("table_catalog"),
                                                lit("/"), col("table_schema"), lit("/"), col("table_name")))
                             .select("key", "table_catalog", "table_schema", "table_name", "table_type", "table_owner", "data_source_format", "secondary_storage_location"))
        managed_tables = managed_tables_df.collect()
        describe_tables = []
        for managed_table in managed_tables:
            describe_tables.append((managed_table["key"], "DESCRIBE DETAIL {key}".format(key=managed_table["key"])))
        describe_tables_df = (functools.reduce(lambda x, y: x.union(y), self.commandExecutor.parallelize_method_over_iterable(describe_tables))
                              .selectExpr("key", "location as primary_storage_location"))
        return (managed_tables_df.alias("s").join(describe_tables_df.alias("t"), managed_tables_df.key == describe_tables_df.key).selectExpr("s.*", "t.primary_storage_location"))

    def __get_external_tables(self) -> DataFrame:
        external_tables_df = (self.spark.read.format("delta").table("system.information_schema.tables").filter(
            f"table_schema<>'information_schema' and table_type='EXTERNAL' and table_owner <> 'System user' and table_catalog <> '__databricks_internal' and table_catalog = 'main'")
                             .withColumn("key", concat(lit("`"), col("table_catalog"), lit("`.`"), col("table_schema"), lit("`.`"), col("table_name"), lit("`")))
                             .select("key", "table_catalog", "table_schema", "table_name", "table_type", "table_owner", "data_source_format"))

        external_tables = external_tables_df.collect()
        show_create_tables = []
        for external_table in external_tables:
            show_create_tables.append((external_table["key"], "SHOW CREATE TABLE {key}".format(key=external_table["key"])))
        create_tables_df = (functools.reduce(lambda x, y: x.union(y), self.commandExecutor.parallelize_method_over_iterable(show_create_tables))
                            .selectExpr("key", "createtab_stmt as createtab_stmt"))
        return (external_tables_df.alias("s").join(create_tables_df.alias("t"), external_tables_df.key == create_tables_df.key).selectExpr("s.*", "t.createtab_stmt"))

    def __get_table_privileges(self, managed_tables: DataFrame, external_tables: DataFrame) -> DataFrame:
      tables = managed_tables.select("table_catalog", "table_schema", "table_name").union(external_tables.select("table_catalog", "table_schema", "table_name"))
      table_privileges = self.spark.read.format("delta").table("`system`.information_schema.table_privileges")
      return (tables.alias("t").join(table_privileges.alias("p"), (tables.table_catalog == table_privileges.table_catalog) & (tables.table_schema== table_privileges.table_schema) 
                                     & (tables.table_name== table_privileges.table_name)).select("p.*"))

    def export(self):
        # Export the managed tables
        managed_tables = self.__get_managed_tables()
        managed_tables.write.format("delta").mode("overwrite").save(self.meta_storage_path + "/managed_tables")
        # Export the external tables
        external_tables = self.__get_external_tables()
        external_tables.write.format("delta").mode("overwrite").save(self.meta_storage_path + "/external_tables")
        # Export the permissions
        table_privileges = self.__get_table_privileges(managed_tables, external_tables)
        external_tables.write.format("delta").mode("overwrite").save(self.privileges_storage_path)

# COMMAND ----------

exporter= TablesExporter(spark, storage_location)
exporter.export()
