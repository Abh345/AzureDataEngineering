# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes() #it will give dbutils scope name 

# COMMAND ----------

dbutils.secrets.list('formula1-scope')  #this will tell the name of azure key vault name

# COMMAND ----------

# MAGIC %md
# MAGIC ![image](files/Screenshot_2023_07_15_032617.png)

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope',key='storage-account-secret-access-key')

# COMMAND ----------


