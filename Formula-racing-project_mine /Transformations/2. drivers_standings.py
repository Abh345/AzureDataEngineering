# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/common_functions"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

driver_standing_df=race_results_df.groupBy("race_year","driver_name","driver_nationality","team").agg(sum('points').alias('points'),count(when(race_results_df.position==1,"Win")).alias("Wins"))

# COMMAND ----------

from pyspark.sql.window import Window
windowPartition = Window.partitionBy("race_year").orderBy(col("points").desc(),col("Wins").desc())

# COMMAND ----------

final_df=driver_standing_df.withColumn("Rank",rank().over(windowPartition))

# COMMAND ----------

#display(final_df)

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
final_df.write.format("parquet").mode('overwrite').saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------


