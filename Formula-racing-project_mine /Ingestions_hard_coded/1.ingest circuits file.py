# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

path="abfss://demo@dgrgreee.dfs.core.windows.net/circuits.csv"

# COMMAND ----------

circuits_df=spark.read.format("csv").load(path,header='True')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC #Defnining schema 

# COMMAND ----------

circuits_df=spark.read.format("csv").schema(circuits_schema).load(path,header='True')

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_selected_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe i.e audit column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

path_to_write="abfss://processed@dgrgreee.dfs.core.windows.net/circuits"

# COMMAND ----------

circuits_final_df.write.format("parquet").mode('overwrite').save(path_to_write,header='True')

# COMMAND ----------

#reading parquet files -- u shoudl not provide schema if you write the data to data lake as schema already applied so we should not give schema to it .other wise it will throw nulls
circuits_df_parquet=spark.read.format("parquet").load(path='abfss://processed@dgrgreee.dfs.core.windows.net/circuits',header='True')

# COMMAND ----------

display(circuits_df_parquet)

# COMMAND ----------


