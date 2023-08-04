# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC aggregation --- logic implemetions for inc. load

# COMMAND ----------

race_results_df=spark.read.format("delta").load(f"{presentation_folder_path}/race_results")

# COMMAND ----------

#race_results_df.columns

# COMMAND ----------

#this will give me unique list containing unique race_years
list_year=race_results_df.select("race_year").distinct().collect()
race_results_list=[]
for i in list_year:
    race_results_list.append(i.race_year)

# COMMAND ----------

race_results_df_final=race_results_df.filter(race_results_df["race_year"].isin(race_results_list))

# COMMAND ----------

driver_standing_df=race_results_df_final.groupBy("race_year","driver_name","driver_nationality").agg(sum('points').alias('points'),count(when(race_results_df_final.position==1,"Win")).alias("Wins"))

# COMMAND ----------

from pyspark.sql.window import Window
windowPartition = Window.partitionBy("race_year").orderBy(col("points").desc(),col("Wins").desc())

# COMMAND ----------

final_df=driver_standing_df.withColumn("Rank",rank().over(windowPartition))

# COMMAND ----------

#display(final_df)

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
#final_df.write.format("parquet").mode('overwrite').saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

#overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Writing data in INC. delta format 

# COMMAND ----------

db_name="f1_presentation"
table_name="driver_standings"
folder_path=presentation_folder_path
partition_column="race_year"
merge_condition="tgt.driver_name=src.driver_name AND tgt.race_year = src.race_year AND tgt.driver_nationality=src.driver_nationality" 

# COMMAND ----------

merge_delta_data(final_df, db_name, table_name, folder_path, merge_condition, partition_column)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings;

# COMMAND ----------


