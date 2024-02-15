-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##create managed table

-- COMMAND ----------

CREATE TABLE main.default.org
(
  deptcode  INT,
  deptname  STRING,
  location  STRING
);

INSERT INTO main.default.org VALUES
  (10, 'FINANCE', 'EDINBURGH'),
  (20, 'SOFTWARE', 'PADDINGTON'),
  (30, 'SALES', 'MAIDSTONE'),
  (40, 'MARKETING', 'DARLINGTON'),
  (50, 'ADMIN', 'BIRMINGHAM');

-- COMMAND ----------

select * from main.default.org

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##deep clone manged table to secondary catalog (for DR), this could be on a secondary region/workspace with the same catalog name, here using the following example for simplicity 

-- COMMAND ----------

CREATE OR REPLACE TABLE uc_migration.default.org
DEEP CLONE main.default.org;

-- COMMAND ----------

INSERT INTO main.default.org VALUES
  (60, 'IT', 'LOSANGELES');

-- COMMAND ----------

select * from main.default.org

-- COMMAND ----------

CREATE OR REPLACE TABLE uc_migration.default.org
DEEP CLONE main.default.org;

-- COMMAND ----------

select * from uc_migration.default.org

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Disaster happens, Databricks services failover to uc_migration.default.org

-- COMMAND ----------

INSERT INTO uc_migration.default.org VALUES
  (70, 'R&D', 'SANFRANSISCO');

-- COMMAND ----------

DELETE FROM uc_migration.default.org WHERE deptcode = 20

-- COMMAND ----------

select * from uc_migration.default.org

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Disaster is over, let's failback to main.default.org

-- COMMAND ----------

select * from main.default.org

-- COMMAND ----------

CREATE OR REPLACE TABLE main.default.org
DEEP CLONE uc_migration.default.org;

-- COMMAND ----------

select * from main.default.org

-- COMMAND ----------

describe history main.default.org
