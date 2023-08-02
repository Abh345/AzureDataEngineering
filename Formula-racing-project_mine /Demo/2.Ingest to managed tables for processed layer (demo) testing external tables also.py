# Databricks notebook source
# MAGIC %sql
# MAGIC --way - to create managed database 
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed_testing_managed
# MAGIC LOCATION "abfss://demo@formula1racing1307.dfs.core.windows.net/testing"
# MAGIC --this database will contain MANAGED tables

# COMMAND ----------

# MAGIC %sql
# MAGIC --way - to create external database 
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed_testing_external
# MAGIC LOCATION "abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external"
# MAGIC --this database will contain EXTERNAL tables

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists f1_test cascade
# MAGIC -- it will remove the folder f1_processed_testing as it is managed table

# COMMAND ----------

# MAGIC %sql
# MAGIC --way-2 best way to create external table 
# MAGIC drop TABLE IF EXISTS f1_processed_testing_external.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed_testing_external.circuits(circuitId INT,
# MAGIC circuitRef STRING,
# MAGIC name STRING,
# MAGIC location STRING,
# MAGIC country STRING,
# MAGIC lat DOUBLE,
# MAGIC lng DOUBLE,
# MAGIC alt INT,
# MAGIC url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC Location 'abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external' 
# MAGIC -- here above loaction is manadatory to give so that when data got written into adls we can use select command in sql to see results .

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed_testing_managed.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists f1_processed_testing_external_tables cascade
# MAGIC -- it will NOT remove the folder f1_processed_testing_external_tables as it is EXTERNAL table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC f1_processed_testing_managed.circuits;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed_testing_external.circuits;

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC * below is syntax to store data into external tables

# COMMAND ----------

# MAGIC %sql
# MAGIC --desc extended f1_processed.circuit_ex

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.circuit_ex

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database f1_raw   -- here no loaction metioned 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists f1_processed_check

# COMMAND ----------


