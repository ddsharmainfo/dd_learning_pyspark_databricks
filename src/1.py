# Databricks notebook source
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
import configparser


config = configparser.ConfigParser()
config.read('./.conf')
db_connect = config.get('IDE', "db_connect")

if db_connect:
    spark = DatabricksSession.builder.profile("DEFAULT").getOrCreate()
else:
    spark = SparkSession.builder.getOrCreate()





# from pyspark.sql.functions import col, when
# columns = ['id', 'name', 'salary']
# data = [
#     (1, 'E1', 100),
#     (2, 'E2', 200),
#     (3, 'E3', 300),
#     (4, 'E4', 400),
#     (5, 'E5', 500),
#     (6, 'E6', 300)
# ]

# df = spark.createDataFrame(data, columns)
# df = df.withColumn("name_new", when(df["name"] == 'E1', 'DD').when(df["name"] == 'E6', 'Sharma').otherwise(df["name"]))
# df.show()

# #df.filter((df.salary >= 300) & (df.name != 'E6')).show()

# df1 = df
# df1 = df1.withColumn("salary_filter", df["salary"] >= 300).withColumn("name_filter", df["name"] != 'E6')
# df1.show()
# df1 = df1.filter(df1["salary_filter"] == True).filter(df1["name_filter"] == True)
# df1.show()

# df = df.withColumn("a_filtered", df["a"] == "1a")
# # Filter out the rows that do not satisfy the second condition
# df = df.filter(df["a_filtered"] == True)
# # Print the DataFrame
# df.show()