-- Databricks notebook source
-- my query
select team_name,
       count(driver_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points 
       from f1_presentation.calculated_race_results 
       group by team_name
having total_races >=100
order by total_races desc,total_points desc;

-- COMMAND ----------

SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE race_year BETWEEN 2001 AND 2011
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

select *, rank() over(order by avg_points desc) as raank from 
(SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE race_year BETWEEN 2001 AND 2011
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC)as d

-- COMMAND ----------


