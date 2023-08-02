# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

path="abfss://raw@dgrgreee.dfs.core.windows.net/races.csv"

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

race_final_df = df_rename_col.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC #####writing data into adls in parquet using partition by 

# COMMAND ----------

path_to_write='abfss://processed@dgrgreee.dfs.core.windows.net/races'

# COMMAND ----------

race_final_df.write.format("parquet").mode('overwrite').partitionBy('race_year').save(path_to_write,header='True')

# COMMAND ----------

df_read_written_data_to_adls=spark.read.format("parquet").load(path='abfss://processed@dgrgreee.dfs.core.windows.net/races',header='True')

# COMMAND ----------

display(df_read_written_data_to_adls)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### reading specific  year data like 2009

# COMMAND ----------

df_read_written_data_to_adls_for_2009=spark.read.format("parquet").load(path='abfss://processed@dgrgreee.dfs.core.windows.net/races/race_year=2009',header='True')

# COMMAND ----------

df_read_written_data_to_adls_for_2009.show()

# COMMAND ----------

df_read_written_data_to_adls=spark.read.format("parquet").load(path='abfss://processed@dgrgreee.dfs.core.windows.net/races',header='True')

# COMMAND ----------

display(df_read_written_data_to_adls)

# COMMAND ----------

df=df_read_written_data_to_adls
df.printSchema()

# COMMAND ----------

df_filter=df.filter((df["race_year"]==2009 ) &(df.circuit_id==1))
display(df_filter)

# COMMAND ----------


