-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@formula1racing1307.dfs.core.windows.net/processed_layer"

-- COMMAND ----------

-- MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

-- COMMAND ----------

-- MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/common_functions"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * below is syntax to store data into external tables

-- COMMAND ----------

--desc extended f1_processed.circuit_ex

-- COMMAND ----------

--drop table if exists f1_processed.circuit_ex

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database f1_raw   -- here no loaction metioned 

-- COMMAND ----------

drop database if exists f1_processed_check

-- COMMAND ----------


