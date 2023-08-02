# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/common_functions"

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

# COMMAND ----------

path=f"{raw_folder_path}/constructors.json"

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
                                             .withColumn("ingestion_date", current_timestamp()) \
                                             .withColumn("data_source", lit(v_data_source))
                                    

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 Write output to parquet file

# COMMAND ----------

#path_to_write=f'{processed_folder_path}/constructors'

# COMMAND ----------

#constructor_final_df.write.format("parquet").mode('overwrite').save(path_to_write,header='True')
constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
