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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

path=f"{raw_folder_path}/{v_file_date }/qualifying"

# COMMAND ----------

qualifying_df=spark.read.format("json").schema(qualifying_schema).load(path,header='True',multiLine='True')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())\
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

path_to_write=f"{processed_folder_path}/qualifying"

# COMMAND ----------

# MAGIC %md
# MAGIC #####logic to handle INC/ load pattern

# COMMAND ----------

#overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Below cannot handle INcremental load pattern

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(path_to_write)
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ####handle INcremental load pattern using Delta lake 

# COMMAND ----------

db_name="f1_processed"
table_name="qualifying"
folder_path=processed_folder_path
partition_column="race_id"
merge_condition="tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"

# COMMAND ----------

merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) 
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------


