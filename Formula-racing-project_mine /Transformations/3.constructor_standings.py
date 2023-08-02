# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/common_functions"

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

team_standing_df=race_results_df.groupBy("race_year","team").agg(sum('points').alias('points'),count(when(race_results_df.position==1,"Win")).alias("Wins"))

# COMMAND ----------

from pyspark.sql.window import Window
windowPartition = Window.partitionBy("race_year").orderBy(col("points").desc(),col("Wins").desc())

# COMMAND ----------

final_df=team_standing_df.withColumn("Rank",rank().over(windowPartition))

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
final_df.write.format("parquet").mode('overwrite').saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

#display(final_df.sort(col("Rank").desc()))
#display(final_df.sort(desc('race_year'),asc("Rank")))

# COMMAND ----------


