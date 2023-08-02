# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

path="abfss://raw@dgrgreee.dfs.core.windows.net/constructors.json"

# COMMAND ----------

constructors_df=spark.read.format("json").load(path,header='True')

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####defining the schema

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df_schema=spark.read.format("json").schema(constructors_schema).load(path,header='True')

# COMMAND ----------

constructors_df_schema.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe
# MAGIC ##### There are two ways to drop columns : 1. using select and drop . 

# COMMAND ----------

constructor_dropped_df=constructors_df_schema.drop('url')

# COMMAND ----------

constructor_dropped_df.show()

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 Write output to parquet file

# COMMAND ----------

path_to_write='abfss://processed@dgrgreee.dfs.core.windows.net/constructors'

# COMMAND ----------

constructor_final_df.write.format("parquet").mode('overwrite').save(path_to_write,header='True')

# COMMAND ----------


