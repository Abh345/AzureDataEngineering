# Databricks notebook source
# MAGIC %md
# MAGIC **Creating Managed tables on Hive meta store

# COMMAND ----------

data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","",None)]

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()

# COMMAND ----------

df.write.format('parquet').mode('overwrite').saveAsTable('employee_data_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended employee_data_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_data_managed;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists employee_data_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/user/hive/warehouse/employee_data_managed`

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating External tables on Hive meta store

# COMMAND ----------

df.write.format('parquet').mode('overwrite').option('path','/user/hive/warehouse/employee_data_external').saveAsTable('employee_data_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended employee_data_external

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists employee_data_external

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/user/hive/warehouse/employee_data_external`

# COMMAND ----------


