# Databricks notebook source
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

df = spark.read.table("hive_metastore.default.employee")
df.show(5)

# COMMAND ----------

df2 = spark.range(0, 5)
df2.show()

# COMMAND ----------
df3 = spark.range(20, 25)
print('Count in df3 is: ', df3.count())