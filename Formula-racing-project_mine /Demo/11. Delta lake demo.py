# Databricks notebook source
# MAGIC %sql 
# MAGIC drop database f1_demo cascade

# COMMAND ----------

# MAGIC %sql 
# MAGIC create database if not exists f1_demo
# MAGIC location "abfss://demo@formula1racing1307.dfs.core.windows.net/f1_demo"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Day 1

# COMMAND ----------


drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("abfss://raw@formula1racing1307.dfs.core.windows.net/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Day -2

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("abfss://raw@formula1racing1307.dfs.core.windows.net/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #day-3 data 

# COMMAND ----------

from pyspark.sql.functions import *

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("abfss://raw@formula1racing1307.dfs.core.windows.net/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", concat(upper("name.forename"),lit('Hi')).alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##creating DElta table to use merge using sql :

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://demo@formula1racing1307.dfs.core.windows.net/f1_demo/drivers_merge"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Day-1 day merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge as tgt
# MAGIC USING drivers_day1 as upd
# MAGIC ON upd.driverId= tgt.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC     
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate)
# MAGIC   VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ##Day-2 data merge once day 1 data added to traget table 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge as tgt
# MAGIC USING drivers_day2 as upd
# MAGIC ON upd.driverId= tgt.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC     
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate)  -- here we can also insert all like INSERT *
# MAGIC   VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ####pyspark way of inserting data to target table 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://demo@formula1racing1307.dfs.core.windows.net/f1_demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------


