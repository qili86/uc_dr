# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import logging
from collections.abc import Iterable
import concurrent
from delta import *

# COMMAND ----------

def _execute_and_collect(_iterable: Iterable, method: callable) -> list[any]:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        logging.info(f"Default number of threads used: {executor._max_workers}")

        futures = {executor.submit(method, x): x for x in _iterable}
        return [(futures[future], future.result()) for future in concurrent.futures.as_completed(futures)]

def parallelize_method_over_iterable(_iterable: Iterable, method: callable, max_batch_size: int = 500):
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
        return _execute_and_collect(_iterable, method)
    else:
        logging.info("We are batching the parallelize_method_over_iterable request.")
        output = []
        for i in range(0, len(_iterable), max_batch_size):
            batch = _iterable[i:i + max_batch_size]
            output.extend(_execute_and_collect(batch, method))

        return output


# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "/tmp/testing_sb_dr/output")
dbutils.widgets.text("storageLocationForBatchInfo", "/tmp/testing_sb_dr/batches")
storageLocation = dbutils.widgets.get("storageLocation")
storageLocationForBatchInfo = dbutils.widgets.get("storageLocationForBatchInfo")
print(storageLocation)
print(storageLocationForBatchInfo)

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, storageLocation):
  raise Exception("No export information available at this location: " + storageLocation)

# COMMAND ----------

state_df = spark.read.format("delta").load(storageLocation)
if DeltaTable.isDeltaTable(spark, storageLocationForBatchInfo):
  processed_batch_ids_df = spark.read.format("delta").load(storageLocationForBatchInfo)
  state_df.alias("new").join(processed_batch_ids_df.alias("old"), state_df.batchId == processed_batch_ids_df.batchId, "left_anti").select("new.*")

# COMMAND ----------


