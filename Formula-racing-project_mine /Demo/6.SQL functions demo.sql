-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select * from f1_processed.drivers limit 10;

-- COMMAND ----------

select concat(driver_ref,"-",code) as concated_columns,driver_id,drivers.* from drivers;

-- COMMAND ----------

select split(name,' ')[0] as first_name, split(name,' ')[1] as last_name from f1_processed.drivers

-- COMMAND ----------

select date_format(dob,'dd-mm-yyyy') as new_date_format from f1_processed.drivers;

-- COMMAND ----------

select * from drivers where dob=(select max(dob) from drivers);

-- COMMAND ----------

select * from 
(select nationality,count(*) as gin from drivers group by nationality ) as d where gin >10;


--having nationality='Mexican';

-- COMMAND ----------

SELECT d.* ,
 rank() OVER( order by gin desc) AS rank
 FROM (select nationality,count(*) as gin from drivers group by nationality ) as d 

-- COMMAND ----------

SELECT d.name,d.nationality,d.dob,
 rank() OVER(PARTITION BY nationality order by d.dob desc) AS rank from drivers as d  ;

-- COMMAND ----------

select race_year,driver_name,driver_nationality,team,sum(points) as total_points from f1_presentation.race_results 
group by race_year,driver_name,driver_nationality,team
order by race_year desc,total_points;

-- COMMAND ----------

create or replace VIEW driver_satnding_view 
AS
select race_year,driver_name,driver_nationality,team,sum(points)as total_points,count(case 
when f.position=1 then "winner"  end) as noOfwins from f1_presentation.race_results as f
group by race_year,driver_name,driver_nationality,team

-- COMMAND ----------

select * from f1_presentation.race_results  where (driver_name='Sebastian Vettel') and (position=1 and race_year=2013)

-- COMMAND ----------

select d.*,rank() over(partition by race_year order by total_points desc,noOfwins desc) as rank  from driver_satnding_view as d;

-- COMMAND ----------


