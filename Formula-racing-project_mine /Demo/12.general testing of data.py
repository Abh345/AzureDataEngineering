# Databricks notebook source
data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("James", "Sales", 3000)
  ]

# Create DataFrame
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

df.distinct().show()  #handling with duplicate

# COMMAND ----------

mf=df.dropDuplicates(["department","department","salary"])#handling with duplicate  

# COMMAND ----------

mf.show()

# COMMAND ----------


