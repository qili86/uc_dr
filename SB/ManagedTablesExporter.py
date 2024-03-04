# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("meta_storage_location", "/tmp/testing_sb_deepclone/dr1/meta")
dbutils.widgets.text("data_storage_location", "/tmp/testing_sb_deepclone/dr1/data")
dbutils.widgets.text("metastore_type", "uc")

meta_storage_location = dbutils.widgets.get("meta_storage_location")
data_storage_location = dbutils.widgets.get("data_storage_location")
metastore_type = dbutils.widgets.get("metastore_type")

print(meta_storage_location)
print(data_storage_location)
print(metastore_type)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import logging
from collections.abc import Iterable
import concurrent
from delta import *


class CommandExecutor:
    """
        A class to run spark commands in parallel.
    """

    def __init__(self, spark):
        self.spark = spark

    def _spark_command_execute(self, key, command) -> DataFrame:
        return self.spark.sql(command).withColumn("key", lit(key))

    def _execute_and_collect(self, _iterable: Iterable) -> list[any]:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            logging.info(f"Default number of threads used: {executor._max_workers}")
            futures = {executor.submit(self._spark_command_execute, x[0], x[1]): x for x in _iterable}
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


class ManagedTablesExporter:
    """
        A class to export the managed table definitions.
    """

    def __init__(self, spark, data_storage_location, meta_storage_location, metastore_type):
        self.spark = spark
        self.data_storage_location = data_storage_location
        self.meta_storage_location = meta_storage_location
        self.metastore_type = metastore_type
        self.managed_tables_storage_path = self.meta_storage_location + "/" + "managed_tables"
        self.managed_tables_privileges_storage_path = self.meta_storage_location + "/" + "managed_tables_priviliges"
        self.commandExecutor = CommandExecutor(self.spark)

    def __get_uc_managed_tables(self) -> DataFrame:
        managed_tables_df = (self.spark.read.format("delta").table("system.information_schema.tables").filter(
            f"table_schema<>'information_schema' and table_type='MANAGED' and table_owner <> 'System user' and table_catalog <> '__databricks_internal' and table_catalog = 'main' and data_source_format = 'DELTA'")
                             .withColumn("key", concat(lit("`"), col("table_catalog"), lit("`.`"), col("table_schema"),
                                                       lit("`.`"), col("table_name"), lit("`")))
                             .withColumn("secondary_storage_location",
                                         concat(lit(self.data_storage_location), lit("/"), col("table_catalog"),
                                                lit("/"),
                                                col("table_schema"), lit("/"), col("table_name")))
                             .select("key", "table_catalog", "table_schema", "table_name", "table_type", "table_owner",
                                     "data_source_format", "secondary_storage_location"))
        managed_tables = managed_tables_df.collect()
        describe_tables = []
        for managed_table in managed_tables:
            describe_tables.append((managed_table["key"], "DESCRIBE DETAIL {key}".format(key=managed_table["key"])))
        describe_tables_df = (functools.reduce(lambda x, y: x.union(y), self.commandExecutor.parallelize_method_over_iterable(describe_tables))
                              .selectExpr("key", "location as primary_storage_location"))
        return managed_tables_df.alias("s").join(describe_tables_df.alias("t"), managed_tables_df.key == describe_tables_df.key).selectExpr("s.*", "t.primary_storage_location")

    def __get_hms_managed_tables(self) -> DataFrame:
        ...

    def export(self):
        if metastore_type.lower() == "uc":
          managed_tables = self.__get_uc_managed_tables()
          table_priviliges = spark.read.format("delta").table("`system`.information_schema.table_privileges")
          # Filter privileges only for managed tables
          priviliges = (managed_tables.alias("t").join(table_priviliges.alias("p"), (managed_tables.table_catalog == table_priviliges.table_catalog) & (managed_tables.table_schema== table_priviliges.table_schema) & (managed_tables.table_name== table_priviliges.table_name)).select("p.*"))
          managed_tables_df.write.format("delta").mode("overwrite").save(self.managed_tables_storage_path)
          priviliges.write.format("delta").mode("overwrite").save(self.managed_tables_privileges_storage_path)
        else:
            ...

# COMMAND ----------

exporter= ManagedTablesExporter(spark, data_storage_location, meta_storage_location, managed_tables_type)
exporter.export()

# COMMAND ----------


