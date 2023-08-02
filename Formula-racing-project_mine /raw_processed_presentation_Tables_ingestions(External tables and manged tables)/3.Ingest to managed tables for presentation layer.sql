-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://presentation@formula1racing1307.dfs.core.windows.net/presentation_layer"

-- COMMAND ----------

-- MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

-- COMMAND ----------

-- MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/common_functions"

-- COMMAND ----------

--select * from f1_presentation.driver_standings;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit('Success')

-- COMMAND ----------


