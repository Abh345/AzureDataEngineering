# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "/Users/1964295@tcs.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results") 

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df=race_results_df.filter("race_year=2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

#count() count_distict() and sum()
#driver_name,points,race_name
# using demo_df as from 2020 rec. only .
ff=demo_df.groupBy("driver_name").sum('points')

# COMMAND ----------

display(ff)

# COMMAND ----------

#multiple agg.
gg=demo_df.groupBy("race_year","driver_name").agg(sum('points').alias('points'),count('race_name').alias('count_of_races'),max('points'),min('points'),avg('points'))

# COMMAND ----------

#Using Window func.
windowPartition_1=Window.partitionBy("race_year").orderBy(desc("points"))

# COMMAND ----------

final_df=gg.withColumn("rank",rank().over(windowPartition_1))
#here points col will auto add values 

# COMMAND ----------

gg=demo_df.groupBy("race_year","driver_name").agg(sum('points').alias('points'))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #WINDOW Func

# COMMAND ----------

demo_df=race_results_df.filter("race_year=2020 or race_year=2019")
#demo_df=race_results_df.filter("race_year in (2020,2019)")

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

windowPartition = Window.partitionBy("race_year").orderBy(desc("points"))

# COMMAND ----------

final_df=gg.withColumn("Rank",rank().over(windowPartition))

# COMMAND ----------

display(final_df)

# COMMAND ----------


