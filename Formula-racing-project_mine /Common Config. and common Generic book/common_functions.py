# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC ###re arranging column logic

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    for i in input_df.columns:
        if i!=partition_column:
            column_list.append(i)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Logic to handle INc. load

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df=re_arrange_partition_column(input_df,partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##handle INC. laod using delta and merge/upsert concept 

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition) \
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------


