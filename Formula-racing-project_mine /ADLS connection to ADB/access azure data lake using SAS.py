# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

sas_token=dbutils.secrets.get(scope='formula1-scope_sas_token',key='storage-account-secret-access-key-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dgrgreee.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dgrgreee.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dgrgreee.dfs.core.windows.net", sas_token)

# COMMAND ----------

path="abfss://demo@dgrgreee.dfs.core.windows.net"

# COMMAND ----------

df=spark.read.format("csv").load(path,header='True', inferSchema='True')

# COMMAND ----------

display(df)

# COMMAND ----------


