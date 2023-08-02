# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "/Users/1964295@tcs.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

# COMMAND ----------

cricuits_df=spark.read.parquet(f"{processed_folder_path}/circuits").filter('circuit_id<70')

# COMMAND ----------

race_df=spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

race_df_filter=race_df.filter(race_df.race_year==2019)


# COMMAND ----------

display(race_df_filter)

# COMMAND ----------

join_circits_races_df=cricuits_df.join(race_df_filter,cricuits_df.circuit_id==race_df_filter.circuit_id,'inner')

# COMMAND ----------

display(join_circits_races_df)

# COMMAND ----------

join_circits_races_df.select(cricuits_df.name.alias('cicuits_name'),race_df.name.alias("rcae_df")).show()

# COMMAND ----------

join_circits_races_df_left=cricuits_df.join(race_df_filter,cricuits_df.circuit_id==race_df_filter.circuit_id,'left')

# COMMAND ----------

display(join_circits_races_df_left)

# COMMAND ----------

join_circits_races_df_full=cricuits_df.join(race_df_filter,cricuits_df.circuit_id==race_df_filter.circuit_id,'full')

# COMMAND ----------

display(join_circits_races_df_full)

# COMMAND ----------


