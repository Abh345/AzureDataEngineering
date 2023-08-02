# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

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

path="abfss://raw@dgrgreee.dfs.core.windows.net/qualifying"

# COMMAND ----------

qualifying_df=spark.read.format("json").schema(qualifying_schema).load(path,header='True',multiLine='True')

# COMMAND ----------

display(qualifying_df)

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
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://processed@dgrgreee.dfs.core.windows.net/qualifying")

# COMMAND ----------

df=spark.read.format("parquet").load(path='abfss://processed@dgrgreee.dfs.core.windows.net/qualifying',header='True')
display(df)

# COMMAND ----------


