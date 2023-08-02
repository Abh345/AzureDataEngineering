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

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/common_functions"

# COMMAND ----------

# MAGIC %run "/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Common Config. and common Generic book/configuration"

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

path=f"{raw_folder_path}/{v_file_date}/circuits.csv"

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
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe i.e audit column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

#path_to_write=f"{processed_folder_path}/circuits"

# COMMAND ----------

#circuits_final_df.write.format("parquet").mode('overwrite').save(path_to_write,header='True')
#circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("circuits") #managed table f1_processed database we have to create as metioned on 2.Ingest to managed tables for processed layer notebook 

#External tables best way to follow way -2 to create external tables .
path111='/Users/abhikushal1307@outlook.com/Formula-racing-project_mine/Demo/2.Ingest to managed tables for processed layer (demo) testing'
pathxx='abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_tables'

#req-1 pathxx=f1_processed_testing_external_tables
#circuits_final_df.write.mode("overwrite").option('path',pathxx).format("parquet").saveAsTable("f1_processed_testing_external.circuits") # here it will write data into diff folder 

pathxy='abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external/circuits'
#pathxy='abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external'
#req.-2
#way-1
circuits_final_df.write.mode("overwrite").option('path',pathxy).format("parquet").saveAsTable("f1_processed_testing_external.circuits")

#way-2
circuits_final_df.write.mode("overwrite").format("parquet").save(pathxy)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed_testing_external.circuits;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed_testing_external.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed_testing_managed.circuits

# COMMAND ----------


