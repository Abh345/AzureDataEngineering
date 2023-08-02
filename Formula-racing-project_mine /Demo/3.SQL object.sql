-- Databricks notebook source
create database if not exists demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database demo

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES;  --here it will show list of tables under current database only

-- COMMAND ----------

describe database extended default

-- COMMAND ----------

use demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Manged Tables Example 

-- COMMAND ----------

-- MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

-- COMMAND ----------

-- MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/common_functions"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(race_result_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.mode("overwrite").format('parquet').saveAsTable("demo.race_results_python") #This will write to Meta store for that we need to use mode("overwrite")

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from demo.race_results_python;
--select * from race_results_python

-- COMMAND ----------

describe  race_results_python ;

-- COMMAND ----------

describe extended race_results_python ;  --more detailed info we can get 

-- COMMAND ----------

create table demo.race_results_by_year_sql
as
select * from demo.race_results_python
where race_year=2020
;

-- COMMAND ----------

describe extended demo.race_results_by_year

-- COMMAND ----------

drop table demo.race_results_by_year_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC External Tables 

-- COMMAND ----------

DROP table if exists race_results_ext_sql;
CREATE TABLE demo.race_results_ext_sql
(
race_id INT,
race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
team STRING,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "abfss://demo@formula1racing1307.dfs.core.windows.net/race_results_ext_sql"
--options (path "abfss://demo@formula1racing1307.dfs.core.windows.net/race_results_ext_sql")

-- COMMAND ----------

SHOW TABLES in demo ;  -- it will show tables in demo database 
--select current_database();

-- COMMAND ----------

DROP TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Writing data to delta lake demo folder 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Here we do only one time insertions ..we should never run this below insert query twice otherwise it will lead to duplicatedata

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_python WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

describe extended demo.race_results_ext_sql;

-- COMMAND ----------

show tables

-- COMMAND ----------

describe extended race_results_python;  --more detailed info we can get from "MANAGED TABLE"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format('parquet').saveAsTable("demo.race_results_python") #This will write to Meta store 
-- MAGIC #as Table already there in meta store so it results into ERROR

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC #writing "MANAGED TABLES in ADLS"
-- MAGIC race_result_df.write.format('parquet').option("path","abfss://demo@formula1racing1307.dfs.core.windows.net/race_results_manaaged_python").saveAsTable("demo.race_results_python_1")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 1. Create Global Temp View
-- MAGIC 1. Create Permanent View

-- COMMAND ----------

show tables in demo 

-- COMMAND ----------

CREATE or REPLACE TEMP VIEW local_view
AS
select * from demo.race_results_python
where race_year=2020;

-- COMMAND ----------

select * from local_view

-- COMMAND ----------

CREATE or REPLACE global TEMP VIEW global_view
AS
select * from demo.race_results_python
where race_year=2012;

-- COMMAND ----------

select * from global_temp.global_view

-- COMMAND ----------

show views

-- COMMAND ----------

show tables in global_temp   --- this will give list of all views like global, local and permanent view as well 

-- COMMAND ----------

drop view temp_view_hai;

-- COMMAND ----------

drop view global_temp.temp_view_hai_1

-- COMMAND ----------

select current_database()

-- COMMAND ----------

--permanent view
-- switch it to demo database which is created by us.we should not use default database 
use demo  ;
CREATE or REPLACE VIEW demo.permanet_view  -- it always good practise to use db.table_name
AS
select * from demo.race_results_python
where race_year=2010;

-- COMMAND ----------

drop view if exists default.permanet_view

-- COMMAND ----------

show tables in demo 

-- COMMAND ----------


