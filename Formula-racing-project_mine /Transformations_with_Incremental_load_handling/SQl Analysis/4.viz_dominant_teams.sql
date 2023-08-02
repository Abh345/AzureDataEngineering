-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant Formula 1 Teams of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
select *, rank() over(order by avg_points desc) as team_rank from
(select team_name,
       count(driver_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points 
       from f1_presentation.calculated_race_results 
       group by team_name
having total_races >=100
order by total_races desc,total_points desc) as d;


-- COMMAND ----------

select * from v_dominant_teams where team_rank<=10;

-- COMMAND ----------

select race_year,team_name,
       count(team_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points 
       from f1_presentation.calculated_race_results 
       group by race_year,team_name
having team_name in (select team_name from v_dominant_teams where team_rank<=5 )
order by race_year asc,avg_points desc;

-- COMMAND ----------

select race_year,team_name,
       count(team_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points 
       from f1_presentation.calculated_race_results 
       group by race_year,team_name
having team_name in (select team_name from v_dominant_teams where team_rank<=5 )
order by race_year asc,avg_points desc;

-- COMMAND ----------

select race_year,team_name,
       count(team_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points 
       from f1_presentation.calculated_race_results 
       group by race_year,team_name
having team_name in (select team_name from v_dominant_teams where team_rank<=5 )
order by race_year asc,avg_points desc;

-- COMMAND ----------


