# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

#using secerts to secure access key 
keys_encrypted=dbutils.secrets.get(scope='formula1-scope',key='storage-account-secret-access-key')


# COMMAND ----------

#access key 
spark.conf.set("fs.azure.account.key.dgrgreee.dfs.core.windows.net",keys_encrypted)

# COMMAND ----------

path="abfss://demo@dgrgreee.dfs.core.windows.net/"

# COMMAND ----------

dbutils.fs.ls(path)

# COMMAND ----------

df=spark.read.format("csv").load(path,header='True', inferSchema='True')

# COMMAND ----------

df.show()

# COMMAND ----------

path1="abfss://demo@dgrgreee.dfs.core.windows.net/inp"
df1=spark.read.format("csv").load(path1,header='True', inferSchema='True')
display(df1)

# COMMAND ----------


