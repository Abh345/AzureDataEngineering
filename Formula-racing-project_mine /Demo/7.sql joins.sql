-- Databricks notebook source
create or replace view d_race_2018 
as 
select driver_name,driver_nationality,race_year from f1_presentation.driver_standings where race_year=2018;

-- COMMAND ----------

create or replace view d_race_2020
as 
select driver_name,driver_nationality,race_year from f1_presentation.driver_standings where race_year=2020;


-- COMMAND ----------

select * from d_race_2018 ;

-- COMMAND ----------

select * from d_race_2020;

-- COMMAND ----------

select * from d_race_2020 join d_race_2018 on d_race_2020.driver_name=d_race_2018.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Anti join here is useful to get those records which are not rpesent on right side of table

-- COMMAND ----------

select * from d_race_2020 anti join d_race_2018 on d_race_2020.driver_name=d_race_2018.driver_name;

-- COMMAND ----------

select * from f1_presentation.race_results;

-- COMMAND ----------


