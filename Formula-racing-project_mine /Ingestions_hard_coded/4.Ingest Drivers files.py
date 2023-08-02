# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

path="abfss://raw@dgrgreee.dfs.core.windows.net/drivers.json"

# COMMAND ----------

race_df=spark.read.format("json").load(path,header='True')

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####defining the schema

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

race_df_schema=spark.read.format("json").schema(drivers_schema).load(path,header='True')

# COMMAND ----------

race_df_schema.printSchema()

# COMMAND ----------

display(race_df_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

drivers_with_columns_df=race_df_schema.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp())\
.withColumn('name',concat(race_df_schema.name.forename,lit(' '),race_df_schema.name.surname))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

path_to_write='abfss://processed@dgrgreee.dfs.core.windows.net/drivers'

# COMMAND ----------

drivers_final_df.write.format("parquet").mode('overwrite').save(path_to_write,header='True')
