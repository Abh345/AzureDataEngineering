# Databricks notebook source
# MAGIC %md
# MAGIC ##### Inserting data from raw to Hive metastore (external tables)
# MAGIC

# COMMAND ----------

v_result = dbutils.notebook.run("1.create raw tables(External tables)",0)

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Inserting data from Processed to Hive metastore (managed tables)
# MAGIC

# COMMAND ----------

v_result = dbutils.notebook.run("2.Ingest to managed tables for processed layer",0)

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Inserting data from Presentation to Hive metastore (managed tables)
# MAGIC

# COMMAND ----------

v_result = dbutils.notebook.run("3.Ingest to managed tables for presentation layer",0)

# COMMAND ----------

v_result

# COMMAND ----------


