# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

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

path="abfss://raw@dgrgreee.dfs.core.windows.net/pit_stops.json"

# COMMAND ----------

pit_stops_df=spark.read.format("json").schema(pit_stops_schema).load(path,header='True',multiLine='True')

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

path_to_write='abfss://processed@dgrgreee.dfs.core.windows.net/pit_stops'

# COMMAND ----------

final_df.write.format("parquet").mode('overwrite').save(path_to_write,header='True')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading dataframe for testing 

# COMMAND ----------


df=spark.read.format("parquet").load(path=path_to_write,header='True')

# COMMAND ----------

display(df)

# COMMAND ----------


