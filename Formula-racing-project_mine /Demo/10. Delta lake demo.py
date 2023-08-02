# Databricks notebook source
x='abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external'

# COMMAND ----------

circuits_df=spark.read.format("parquet").option("inferSchema", True).load(x,header='True')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating External delta table 
# MAGIC ####creating managed delta table 

# COMMAND ----------

# MAGIC %sql
# MAGIC --way -1 to create external database
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed_testing_external_delta
# MAGIC LOCATION "abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC --way -1 to create managed database
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed_testing_managed_delta
# MAGIC LOCATION "abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_managed_delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table f1_processed_testing_external_delta.circuits

# COMMAND ----------

#writing to external table in external databse 
y='abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta/circuits'
circuits_df.write.mode("overwrite").option('path',y).format("delta").saveAsTable("f1_processed_testing_external_delta.circuits")

# COMMAND ----------

#writing to managed table in managed databse 
circuits_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed_testing_managed_delta.circuits")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed_testing_external_delta.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed_testing_external_delta.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed_testing_managed_delta.circuits

# COMMAND ----------

#once we write data in delta lake format we can read it by two ways either by spark.read api or by sql 

# COMMAND ----------

# MAGIC %sql   --#reading data from extrenal delta table using sql and store it into circuits_external table i.e on diff. table .
# MAGIC CREATE table f1_processed_testing_external_delta.circuits_external
# MAGIC using DELTA
# MAGIC Location 'abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta/circuits' 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed_testing_external_delta.circuits_external

# COMMAND ----------

#f1_processed_testing_external_delta.circuits_external_partitioned

# COMMAND ----------

#external table partioning
y='abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta/circuits_external_partitioned'
circuits_df.write.mode("overwrite").partitionBy("country").option('path',y).format("delta").saveAsTable("f1_processed_testing_external_delta.circuits_external_partitioned")

# COMMAND ----------

#managed tbale partitioning
circuits_df.write.mode("overwrite").partitionBy("country").format("delta").saveAsTable("f1_processed_testing_managed_delta.circuits_managed_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_processed_testing_external_delta.circuits_external_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete From Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC ######## DATA LAKE  vs DELTA LAKE 

# COMMAND ----------

# MAGIC %sql
# MAGIC ---IN DATA LAKE UPDATE DELETE MERGE AND UPSERT NOT SUPPORTED .
# MAGIC UPDATE f1_processed_testing_external.circuits
# MAGIC set country='United states of america'
# MAGIC where country='USA'

# COMMAND ----------

# MAGIC %sql
# MAGIC ---IN DELTA LAKE UPDATE DELETE MERGE AND UPSERT SUPPORTED .
# MAGIC UPDATE f1_processed_testing_external_delta.circuits
# MAGIC set country='United states of america'
# MAGIC where country='USA'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed_testing_external_delta.circuits;

# COMMAND ----------

df2 = spark.sql('select * from f1_processed_testing_external_delta.circuits')

# COMMAND ----------

u='abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta/circuits_extrenal_after_updating_delta_table'

df2.write.mode("overwrite").option('path',u).format("delta").saveAsTable("f1_processed_testing_external_delta.circuits_extrenal_after_updating_delta_table")    

# here we are writing data to adls after updating delta table 'f1_processed_testing_external_delta.circuits'

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed_testing_external_delta.circuits;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed_testing_external_delta.circuits_extrenal_after_updating_delta_table;

# COMMAND ----------

#python way to doing it 
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta/circuits")
df=deltaTable.update("country='USA'", { "country": "'United states of america'"} ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed_testing_external_delta.circuits

# COMMAND ----------

df1=spark.sql('select * from f1_processed_testing_external_delta.circuits')

# COMMAND ----------

u='abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta/circuits_extrenal_after_updating_delta_table'

df1.write.mode("overwrite").option('path',u).format("delta").saveAsTable("f1_processed_testing_external_delta.circuits_extrenal_after_updating_delta_table") 
#this will be managed table 

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_processed_testing_external_delta.circuits
# MAGIC WHERE country like 'United%';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed_testing_external_delta.circuits

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_processed_testing_external_delta.circuits;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table f1_processed_testing_external_delta.circuits_vaccum as 
# MAGIC select * from f1_processed_testing_external_delta.circuits version as of 6; ---roll back  

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed_testing_external_delta.circuits_vaccum -- lets say if we want to implement GDPR complaince .so it means if user wants his/her data to be removed permanetly and immediatley then we need to use above Cmd 38 step so that no one can restore his/her data based on time travel functionality .

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_processed_testing_external_delta.circuits_vaccum
# MAGIC WHERE country like 'Turkey%';
# MAGIC -- after deleting we should always run the vaccum command in order to delete the data permanently .
# MAGIC -- here we cannot restore the data but still we can see the history of that circuits_vaccum table

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_processed_testing_external_delta.circuits_vaccum  RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed_testing_external_delta.circuits_vaccum;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_processed_testing_external_delta.circuits_vaccum;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed_testing_external_delta.circuits_vaccum version as of 8;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed_testing_external_delta.circuits_vaccum version as of 9;
# MAGIC
# MAGIC -- here you see data but deleted data you cannot see as VACCUM IS Applied 
# MAGIC

# COMMAND ----------

x1='abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta/circuits_vaccum'

# COMMAND ----------

circuits_vaccum_df=spark.read.format("delta").option("inferSchema", True).load(x1,header='True')

# COMMAND ----------

circuits_vaccum_df.count()  #it will aslo applies changes to adls on real time .

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet tables to Delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed_testing_external.drivers_convert_to_delta
# MAGIC USING PARQUET
# MAGIC LOCATION 'abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external/circuits'

# COMMAND ----------

df_c=spark.sql('select * from f1_processed_testing_external.drivers_convert_to_delta')

# COMMAND ----------

u='abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta/drivers_convert_to_delta'

df_c.write.mode("overwrite").option('path',u).format("parquet").saveAsTable("f1_processed_testing_external_delta.drivers_convert_to_delta") 

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed_testing_external_delta.drivers_convert_to_delta

# COMMAND ----------

# MAGIC %md 
# MAGIC # converting parquet to delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta/drivers_convert_to_delta`

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_managed_delta'))

# COMMAND ----------

dbutils.fs.ls('abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta/')

# COMMAND ----------

x=dbutils.fs.ls('abfss://demo@formula1racing1307.dfs.core.windows.net/f1_processed_testing_external_delta')

# COMMAND ----------

x[0].name

# COMMAND ----------

p=[]
for i in range(0,len(x)):
    p.append(x[i].path+x[i].name)
print(p)

# COMMAND ----------


