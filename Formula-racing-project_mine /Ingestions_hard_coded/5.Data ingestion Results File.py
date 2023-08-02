# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

path="abfss://raw@dgrgreee.dfs.core.windows.net/results.json"

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df=spark.read.format("json").schema(results_schema).load(path,header='True')

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted column

# COMMAND ----------

results_final_df = results_with_columns_df.drop("statusId")

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

path_to_write='abfss://processed@dgrgreee.dfs.core.windows.net/results'

# COMMAND ----------

results_final_df.write.format("parquet").mode('overwrite').partitionBy('race_id').save(path_to_write,header='True')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading dataframe for testing 

# COMMAND ----------


df=spark.read.format("parquet").load(path=path_to_write,header='True')

# COMMAND ----------

display(df)

# COMMAND ----------


