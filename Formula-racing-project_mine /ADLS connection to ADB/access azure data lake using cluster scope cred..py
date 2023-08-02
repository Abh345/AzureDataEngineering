# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md 
# MAGIC #No spark config is req. we already done chnages in cluster level 
# MAGIC

# COMMAND ----------

path="abfss://raw@formula1racing1307.dfs.core.windows.net/circuits.csv"

# COMMAND ----------

df=spark.read.format("csv").load(path,header='True', inferSchema='True')
#here is happening as we remove sprak conifg. from cluster 

# COMMAND ----------

display(df)

# COMMAND ----------


