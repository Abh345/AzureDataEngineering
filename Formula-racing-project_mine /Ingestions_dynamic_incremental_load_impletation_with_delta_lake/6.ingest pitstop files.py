# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/common_functions"

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

path=f"{raw_folder_path}/{v_file_date}/pit_stops.json"

# COMMAND ----------

pit_stops_df=spark.read.format("json").schema(pit_stops_schema).load(path,header='True',multiLine='True')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())\
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#path_to_write=f"{processed_folder_path}/pit_stops"

# COMMAND ----------

# MAGIC %md
# MAGIC #####logic to handle INC/ load pattern

# COMMAND ----------

#overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Below cannot handle INcremental load pattern

# COMMAND ----------

#final_df.write.format("parquet").mode('overwrite').save(path_to_write,header='True')
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC ####handle INcremental load pattern using Delta lake 

# COMMAND ----------

db_name="f1_processed"
table_name="pit_stops"
folder_path=processed_folder_path
partition_column="race_id"
merge_condition="tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"

# COMMAND ----------

merge_delta_data(final_df, db_name, table_name, folder_path, merge_condition, partition_column)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) 
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------


