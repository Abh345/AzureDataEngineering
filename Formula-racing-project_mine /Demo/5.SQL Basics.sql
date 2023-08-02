-- Databricks notebook source
show databases;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

select * from f1_processed.drivers limit 10 ;


-- COMMAND ----------

describe f1_processed.drivers

-- COMMAND ----------

select * from f1_processed.drivers where dob >='1990-01-01' order by dob desc,nationality asc;

-- COMMAND ----------


