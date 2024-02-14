-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##create managed table

-- COMMAND ----------

CREATE TABLE main.default.department
(
  deptcode  INT,
  deptname  STRING,
  location  STRING
);

INSERT INTO main.default.department VALUES
  (10, 'FINANCE', 'EDINBURGH'),
  (20, 'SOFTWARE', 'PADDINGTON'),
  (30, 'SALES', 'MAIDSTONE'),
  (40, 'MARKETING', 'DARLINGTON'),
  (50, 'ADMIN', 'BIRMINGHAM');

-- COMMAND ----------

select * from main.default.department

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##deep clone manged table

-- COMMAND ----------

CREATE OR REPLACE TABLE uc_migration.default.department
DEEP CLONE main.default.department;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### add a new column to the original table

-- COMMAND ----------

ALTER TABLE main.default.department
ADD COLUMN new_column STRING;

-- COMMAND ----------

INSERT INTO main.default.department VALUES
  (50, 'ADMIN', 'BIRMINGHAM', '00');

-- COMMAND ----------

select * from main.default.department

-- COMMAND ----------

select * from uc_migration.default.department

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### deep clone carry over the new column

-- COMMAND ----------

CREATE OR REPLACE TABLE uc_migration.default.department
DEEP CLONE main.default.department;

-- COMMAND ----------

select * from uc_migration.default.department

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### deep clone copy data changes incrementally

-- COMMAND ----------

describe history uc_migration.default.department

-- COMMAND ----------

INSERT INTO main.default.department VALUES
  (50, 'ADMIN', 'BIRMINGHAM', '11');

-- COMMAND ----------

CREATE OR REPLACE TABLE uc_migration.default.department
DEEP CLONE main.default.department;

-- COMMAND ----------

describe history uc_migration.default.department

-- COMMAND ----------

DELETE FROM main.default.department WHERE new_column = '11';

-- COMMAND ----------

CREATE OR REPLACE TABLE uc_migration.default.department
DEEP CLONE main.default.department;

-- COMMAND ----------

describe history uc_migration.default.department

-- COMMAND ----------

select * from uc_migration.default.department

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### trying to drop a column

-- COMMAND ----------

ALTER TABLE main.default.department
DROP COLUMN new_column;

-- COMMAND ----------

ALTER TABLE main.default.department SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')


-- COMMAND ----------

ALTER TABLE main.default.department SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')

-- COMMAND ----------

ALTER TABLE main.default.department
DROP COLUMN new_column;

-- COMMAND ----------

select * from main.default.department

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### drop columns will break deep clone

-- COMMAND ----------

CREATE OR REPLACE TABLE uc_migration.default.department
DEEP CLONE main.default.department;
