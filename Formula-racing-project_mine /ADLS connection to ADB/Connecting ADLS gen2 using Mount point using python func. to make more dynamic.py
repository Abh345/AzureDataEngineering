# Databricks notebook source
# MAGIC %md
# MAGIC #Mounting using wasbs protocal and access key 
# MAGIC ## making the notebook more dynamic to all containers

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

def mounting_any_adls_containers(storage_account_name,container_name):
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    accesskey=dbutils.secrets.get(scope='formula1-scope',key='storage-account-secret-access-key')
    dbutils.fs.mount(
  source = f'wasbs://{container_name}@{storage_account_name}.blob.core.windows.net',
  mount_point = f'/mnt/{storage_account_name}/{container_name}',
  extra_configs = {'fs.azure.account.key.dgrgreee.blob.core.windows.net':accesskey}
  )
    mount_point_x=f'/mnt/{storage_account_name}/{container_name}'
    return mount_point_x

# COMMAND ----------

def mounting_any_adls_containers_with_diff_logic(storage_account_name,container_name):
    # Unmount the mount point if it already exists
    # Unmount the mount point if it already exists with diff logic
    for mount in dbutils.fs.mounts():
        if mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" :
           dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    accesskey=dbutils.secrets.get(scope='formula1-scope',key='storage-account-secret-access-key')
    dbutils.fs.mount(
  source = f'wasbs://{container_name}@{storage_account_name}.blob.core.windows.net',
  mount_point = f'/mnt/{storage_account_name}/{container_name}',
  extra_configs = {'fs.azure.account.key.dgrgreee.blob.core.windows.net':accesskey}
  )
    mount_point_x=f'/mnt/{storage_account_name}/{container_name}'
    return mount_point_x

# COMMAND ----------

mount_point_raw=mounting_any_adls_containers('dgrgreee', 'raw')

# COMMAND ----------

mount_point_raw_with_diff_logic=mounting_any_adls_containers_with_diff_logic('dgrgreee', 'raw')

# COMMAND ----------

df_mount_raw_with_diff_logic=spark.read.format("csv").load(path=mount_point_raw_with_diff_logic,header='True', inferSchema='True')
display(df_mount_raw_with_diff_logic)

# COMMAND ----------

df_mount_raw=spark.read.format("csv").load(path=mount_point_raw,header='True', inferSchema='True')

# COMMAND ----------

display(df_mount_raw)

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

# COMMAND ----------

display(dbutils.fs.mounts()
        )

# COMMAND ----------

for mount in dbutils.fs.mounts():
    print(mount.mountPoint)

# COMMAND ----------


