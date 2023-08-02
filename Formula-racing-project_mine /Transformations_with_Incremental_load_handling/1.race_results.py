# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/common_functions"

# COMMAND ----------

#reading races files from processed 
races_df_1 = spark.read.parquet(f"{processed_folder_path}/races") 
races_df=races_df_1.withColumnRenamed('name','race_name').withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

#display(races_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

#display(circuits_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") 

#here above filter condition is imp ...this will help to avoid re processing of already porcessed files .

# COMMAND ----------

# MAGIC %md
# MAGIC ###Joining circuits table to races table 

# COMMAND ----------

race_circuit_joined_data=races_df.join(circuits_df,circuits_df.circuit_id==races_df.circuit_id,'inner').select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

#display(race_circuit_joined_data)

# COMMAND ----------

#display(results_df)

# COMMAND ----------

final_df=results_df.join(race_circuit_joined_data,race_circuit_joined_data.race_id==results_df.race_id,'inner').join(drivers_df,drivers_df.driver_id==results_df.driver_id).join(constructors_df, results_df.constructor_id == constructors_df.constructor_id).select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location,drivers_df.driver_name,drivers_df.driver_number,drivers_df.driver_nationality,results_df.grid,results_df.fastest_lap,results_df.race_time,results_df.points,constructors_df.team,results_df.position,results_df.file_date).withColumn("created_date", current_timestamp()).withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

#final_df.count()

# COMMAND ----------

#display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").sort(final_df.points.desc()))
#display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
#final_df.write.format("parquet").mode('overwrite').save(f"{presentation_folder_path}/race_results",header='True')

# COMMAND ----------

#final_df.write.format("parquet").mode('overwrite').saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Inc. logic 

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(*) from f1_presentation.race_results group by file_date order by file_date desc;

# COMMAND ----------


