# Databricks notebook source
# MAGIC %md
# MAGIC ######1.ingest circuits file

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest circuits file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ######2.Ingest race file

# COMMAND ----------

v_result = dbutils.notebook.run("2.Ingest race file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ######3.ingest constructors files

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest constructors files",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ######4.Ingest Drivers files

# COMMAND ----------

v_result = dbutils.notebook.run("4.Ingest Drivers files",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ######5.ingest Results File

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest Results File",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ######6.ingest pitstop files

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest pitstop files",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ######7.ingest lap times file

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest lap times file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ######8.ingest qualifying file

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest qualifying file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------


