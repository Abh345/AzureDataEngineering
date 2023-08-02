-- Databricks notebook source
select * from f1_presentation.calculated_race_results where race_year between 2011 AND 2020;

-- COMMAND ----------

-- my query
select driver_name,
       count(driver_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points 
       from f1_presentation.calculated_race_results 
       group by driver_name
having total_races >=50
order by total_races desc,total_points desc;

-- COMMAND ----------

-- ramesh query 
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- my query
select count(driver_name)
from f1_presentation.race_results
where driver_name='Lewis Hamilton'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####race_year BETWEEN 2011 AND 2020

-- COMMAND ----------

SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

select driver_name,
       count(driver_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points 
       from (select * from f1_presentation.calculated_race_results where race_year between 2011 AND 2020) as truncated_1
       group by driver_name
having total_races >=50
order by total_races desc,total_points desc;
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####race_year BETWEEN 2001 AND 2010

-- COMMAND ----------

SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

select 10-3 ;

-- COMMAND ----------


