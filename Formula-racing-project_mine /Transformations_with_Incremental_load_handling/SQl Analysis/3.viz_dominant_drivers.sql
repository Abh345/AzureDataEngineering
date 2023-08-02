-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant Formula 1 Drivers of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
select *, rank() over(order by avg_points desc) as rank from
(select driver_name,
       count(driver_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points 
       from f1_presentation.calculated_race_results 
       group by driver_name
having total_races >=50
order by total_races desc,total_points desc) as d;


-- COMMAND ----------

select * from v_dominant_drivers where rank<=10;

-- COMMAND ----------

select race_year,driver_name,
       count(driver_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points 
       from f1_presentation.calculated_race_results 
       group by race_year,driver_name
having driver_name in (select driver_name from v_dominant_drivers where rank<=10)
order by race_year asc,avg_points desc;

-- COMMAND ----------

select race_year,driver_name,
       count(driver_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points 
       from f1_presentation.calculated_race_results 
       group by race_year,driver_name
having driver_name in (select driver_name from v_dominant_drivers where rank<=10)
order by race_year asc,avg_points desc;

-- COMMAND ----------

select race_year,driver_name,
       count(driver_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points 
       from f1_presentation.calculated_race_results 
       group by race_year,driver_name
having driver_name in (select driver_name from v_dominant_drivers where rank<=10)
order by race_year asc,avg_points desc;

-- COMMAND ----------


