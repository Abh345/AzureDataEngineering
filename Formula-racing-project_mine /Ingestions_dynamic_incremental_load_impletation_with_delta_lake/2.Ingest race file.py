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

path=f"{raw_folder_path}/{v_file_date}/races.csv"

# COMMAND ----------

race_df=spark.read.format("csv").load(path,header='True')

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####defining the schema

# COMMAND ----------

#Normal Struct based schema defination 
race_schema=StructType(fields=[StructField("raceid", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitid", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

#ddl based schema defination 
race_schema_DDL = "raceid INT, year INT, round INT, circuitid INT, name STRING,date DATE,time STRING,url STRING"

# COMMAND ----------

race_df=spark.read.format("csv").schema(race_schema_DDL).load(path,header='True')
race_df.printSchema()

# COMMAND ----------

race_df=spark.read.format("csv").schema(race_schema).load(path,header='True')

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####create race timestamp column 

# COMMAND ----------

race_df_add_race_timestamp_1=race_df.withColumn('race_timestamp',to_timestamp(concat(race_df.date,lit(' '),race_df.time),"yyyy-MM-dd HH:mm:ss"))


# COMMAND ----------

#race_df_add_race_timestamp_2=race_df.withColumn('race_timestamp',to_timestamp(concat_ws(' ',race_df.date,race_df.time),"yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

semi_final_race_df=race_df_add_race_timestamp_1

# COMMAND ----------

race_df_add_race_timestamp_1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename columns and drop unnesscary columns 

# COMMAND ----------

df=race_df_add_race_timestamp_1.select("raceid","year","round","circuitid","name","race_timestamp")

# COMMAND ----------

df_rename_col=df.withColumnRenamed('raceid','race_id').withColumnRenamed('year','race_year').withColumnRenamed('circuitid','circuit_id')

# COMMAND ----------

race_final_df = add_ingestion_date(df_rename_col)

# COMMAND ----------

race_final_df=race_final_df.withColumn("file_date", lit(v_file_date)).withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(race_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####writing data into adls in Delta format using partition by 

# COMMAND ----------

#path_to_write=f'{processed_folder_path}/races'

# COMMAND ----------

race_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
