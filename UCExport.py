# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import logging
from collections.abc import Iterable
import concurrent
from delta import *

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "/tmp/testing_sb_dr/export/output")
storageLocation = dbutils.widgets.get("storageLocation")
print(storageLocation)

# COMMAND ----------

class ExportTypes:
    """
    Enum contains the objects that are exportable.
    """
    EXTERNAL_LOCATIONS = "EXTERNAL_LOCATIONS"
    CATALOG = "CATALOG"
    SCHEMA = "SCHEMA"
    TABLES = "TABLES"
    VIEWS = "VIEWS"
    ALL = "ALL"


class Exporter:
    """"
    A driver class to export unity catalog objects.
    """

    def __init__(self, spark, export_types: set[ExportTypes], exclude_catalogs: list[str] = [],
                 exclude_schemas: dict = {},
                 exclude_tables: dict = {}, exclude_external_locations: list[str] = []):
        self.spark = spark
        self.export_types = export_types
        self.exclude_catalogs = exclude_catalogs
        self.exclude_schemas = exclude_schemas
        self.exclude_tables = exclude_tables
        self.exclude_external_locations = exclude_external_locations

    def _spark_command_execute(self, key, command) -> DataFrame:
        return self.spark.sql(command).withColumn("key", lit(key))

    def _execute_and_collect(self, _iterable: Iterable) -> list[any]:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            logging.info(f"Default number of threads used: {executor._max_workers}")
            futures = {executor.submit(self._spark_command_execute, x[0], x[1]): x for x in _iterable}
            return [future.result() for future in concurrent.futures.as_completed(futures)]

    def __get_schemas(self, catalog_name: str, exclude_databases: list[str]):
        df = self.spark.read.format("delta").table("{name}.information_schema.schemata".format(name=catalog_name))\
            .filter(~col("schema_name").isin(exclude_databases))

        schemas_df = (df.withColumn("info", from_json(to_json(struct([df[x] for x in df.columns])), "MAP<STRING,STRING>"))
                      .withColumn("statement_template", lit("CREATE SCHEMA IF NOT EXISTS `<schema_name>` COMMENT '<comment>'"))
                      .withColumn("comment", when(col("comment").isNull(), " ").otherwise(col("comment")))
                      .withColumn("key", concat(col("catalog_name"), lit("."), col("schema_name")))
                      .withColumn("statement", replace(replace(col("statement_template"), lit("<schema_name>"), col("key")),
                                                       lit("<comment>"), col("comment")))
                      .withColumn("export_type", lit("SCHEMA"))
                      .withColumn("priority", lit(2))
                      .selectExpr("key", "export_type", "schema_name as name", "schema_owner as owner", "statement", "info", "priority"))

        return schemas_df
      
    def __get_tables(self, catalog_name: str):
        df = self.spark.read.format("delta").table("{name}.information_schema.tables".format(name=catalog_name)).filter("table_type = 'EXTERNAL'")
        # TODO: exclude tables. Its not that important for now..
        tables_df = (df.withColumn("info", from_json(to_json(struct([df[x] for x in df.columns])), "MAP<STRING,STRING>"))
                      .withColumn("key", concat(col("table_catalog"), lit("."), col("table_schema"), lit("."), col("table_name")))
                      .withColumn("export_type", lit("TABLES"))
                      .withColumn("priority", lit(3))
                      .selectExpr("key", "export_type", "key as name", "table_owner as owner", "info", "priority"))
        return tables_df
      
    def parallelize_method_over_iterable(self, _iterable: Iterable, max_batch_size: int = 500):
        """
        Parallelizes the execution of a callable over each element in an iterable.

        Batching of the processing will occur if the length of the iterable is greater than
        the max_batch size, which is to prevent overloading the driver queue.

        Args:
            _iterable: An iterable collection of elements.
            method: A function to apply to each element in `_iterable`.
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

    def get_storage_credentials(self) -> DataFrame:
        """
        This operation is not supported through this util.
        :return:
        :rtype:
        """
        raise Exception("Storage credentials are not supported through this util. Use TF Exporter or API")

    def get_external_locations(self) -> DataFrame:
        """
        Returns the external locations info from information schema.
        :return:
        :rtype:
        """
        external_location_template = """
            CREATE EXTERNAL LOCATION IF NOT EXISTS `<location_name>` URL '<url_str>' WITH (STORAGE CREDENTIAL `<credential_name>`) COMMENT '<comment>'
            """
        df = self.spark.read.format("delta").table("`system`.information_schema.external_locations") \
            .filter(~col("external_location_name").isin(self.exclude_external_locations))

        external_locations = (
            df.withColumn("info", from_json(to_json(struct([df[x] for x in df.columns])), "MAP<STRING,STRING>"))
            .withColumn("comment", when(col("comment").isNull(), " ").otherwise(col("comment")))
            .withColumn("statement_template", lit(external_location_template))
            .withColumn("statement", replace(replace(replace(
                replace(col("statement_template"), lit("<location_name>"), col("external_location_name")),
                lit("<comment>"), col("comment")), lit("<url_str>"), col("url")), lit("<credential_name>"),
                col("storage_credential_name"))).withColumn("export_type", lit("EXTERNAL_LOCATIONS"))
            .withColumn("key", col("external_location_name"))
            .withColumn("priority", lit(0))
            .selectExpr("key", "export_type", "external_location_name as name", "external_location_owner as owner", "statement", "info", "priority"))

        return external_locations

    def get_catalogs(self) -> DataFrame:
        df = self.spark.read.format("delta").table("`system`.information_schema.catalogs")\
            .filter(~col("catalog_name").isin(self.exclude_catalogs))

        catalogs = (df.withColumn("info", from_json(to_json(struct([df[x] for x in df.columns])), "MAP<STRING,STRING>"))
                    .withColumn("comment", when(col("comment").isNull(), " ").otherwise(col("comment")))
                    .withColumn("statement_template", lit("CREATE CATALOG IF NOT EXISTS `<catalog-name>` COMMENT '<comment>'"))
                    .withColumn("statement", replace(
                        replace(col("statement_template"), lit("<catalog-name>"), col("catalog_name")),
                        lit("<comment>"), col("comment")))
                    .withColumn("export_type", lit("CATALOG")).withColumn("key", col("catalog_name"))
                    .withColumn("priority", lit(1))
                    .selectExpr("key", "export_type", "catalog_name as name", "catalog_owner as owner", "statement", "info", "priority")
                    .filter(col("catalog_name").isin(['main'])))  # TODO: remove this after testing.

        return catalogs

    def get_schemas(self) -> DataFrame:
        catalog_names = self.get_catalogs().select("name").collect()
        databases = []
        for catalog_name in catalog_names:
            # Exclude schemas for this catalog
            exclude_databases = []
            if catalog_name["name"] in self.exclude_schemas:
              exclude_databases = self.exclude_schemas[catalog_name["name"]]
            databases.append(self.__get_schemas(catalog_name["name"], exclude_databases))
        schemas_df = functools.reduce(lambda x, y: x.union(y), databases)
        return schemas_df

    def get_tables(self) -> DataFrame:
        catalog_names = self.get_catalogs().select("name").collect()
        tables = []
        for catalog_name in catalog_names:
            tables.append(self.__get_tables(catalog_name["name"]))
        tables_df = functools.reduce(lambda x, y: x.union(y), tables)
        table_names = []
        for table_name in tables_df.select("name").collect():
            names = table_name["name"].split(".")
            table_names.append((table_name["name"], "SHOW CREATE TABLE `{catalog_name}`.`{schema_name}`.`{table_name}`".format(catalog_name=names[0], schema_name=names[1], table_name=names[2])))

        # get the create table statement for all the tables in the list
        table_statements = self.parallelize_method_over_iterable(table_names)
        table_statements_df = functools.reduce(lambda x, y: x.union(y), table_statements)
        return tables_df.alias("t").join(table_statements_df.alias("s"), tables_df.key == table_statements_df.key).selectExpr("t.key", "t.export_type", "t.name", "t.owner", "s.createtab_stmt as statement", "t.info", "t.priority")

    def get_priviliges(self):
        # priority - Last (999)
        # get the storage credentials priviliges
        # get the external locations priviliges
        # get the catalog priviliges
        # get the schema priviliges
        # get the table priviliges
      ...

    def export(self) -> DataFrame:
        if self.export_types == ExportTypes.EXTERNAL_LOCATIONS:
            return self.get_external_locations()
        elif self.export_types == ExportTypes.CATALOG:
            return self.get_catalogs()
        elif self.export_types == ExportTypes.SCHEMA:
            return self.get_schemas()
        elif self.export_types == ExportTypes.TABLES:
            return self.get_tables()
        elif self.export_types == ExportTypes.VIEWS:
            raise Exception("Not supported yet.") 
        elif self.export_types == ExportTypes.ALL:
            dataframes = [self.get_external_locations(), self.get_catalogs(), self.get_schemas(), self.get_tables()]
            return functools.reduce(lambda x, y: x.union(y), dataframes)
          

# COMMAND ----------

exporter = Exporter(spark, ExportTypes.ALL)
exporter_df = exporter.export()

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, storageLocation):
  state_df = spark.read.format("delta").load(storageLocation)
  # Get the tables that are altered since we processed. 
  table_definitions_changed_df = exporter_df.filter("export_type == 'TABLES'").alias("e").join(state_df.filter("export_type == 'TABLES'").alias("s"), ((exporter_df.key == state_df.key) & (exporter_df.info.last_altered != state_df.info.last_altered))).select("e.*").withColumn("statement", replace(col("statement"), lit("CREATE"), lit("CREATE OR REPLACE")))
  exporter_df = exporter_df.alias("e").join(state_df.alias("s"), exporter_df.key == state_df.key, "left_anti").selectExpr("e.*").union(table_definitions_changed_df)

# COMMAND ----------

print("Number of new changes ", exporter_df.count())

# COMMAND ----------

exporter_df.display()

# COMMAND ----------

exporter_df.withColumn("timestamp", current_timestamp()).withColumn("batchId", col("timestamp").cast("long")).write.format("delta").mode("append").save(storageLocation)
