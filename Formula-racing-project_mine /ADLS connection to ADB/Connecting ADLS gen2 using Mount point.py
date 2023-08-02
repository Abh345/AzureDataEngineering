# Databricks notebook source
# MAGIC %md
# MAGIC #Mounting using wasbs protocal and access key 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

accesskey=dbutils.secrets.get(scope='formula1-scope',key='storage-account-secret-access-key')

# COMMAND ----------

dbutils.fs.mount(
  source = 'wasbs://demo@dgrgreee.blob.core.windows.net',
  mount_point = '/mnt/mountdemo',
  extra_configs = {'fs.azure.account.key.dgrgreee.blob.core.windows.net':accesskey}
  )

# COMMAND ----------

df_mount=spark.read.format("csv").load(path='/mnt/mountdemo',header='True', inferSchema='True')

# COMMAND ----------

display(df_mount)

# COMMAND ----------

# MAGIC %md
# MAGIC #Mounting using Service principal 

# COMMAND ----------

# MAGIC %md
# MAGIC ##WE DO NOT HAVE ACCESS TO SERVICE PRINICIPAL

# COMMAND ----------

#This is config for mouting using service principal 

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)
