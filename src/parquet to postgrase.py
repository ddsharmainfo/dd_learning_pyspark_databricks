# Databricks notebook source

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.window import Window
import configparser
import json
import os

DIR = os.path.dirname(__file__)

print(DIR)
CONFIG_FILE = os.path.join(DIR, '../config/configs.conf')
INPUT_FILE_PATH = os.path.join(DIR, './parquet_nested.parquet')
ENVIRONMENT = 'Dev'

print(CONFIG_FILE)
print(INPUT_FILE_PATH)

config = configparser.ConfigParser()
config.read(CONFIG_FILE)
db_connect = config.get('IDE', "db_connect")
print(db_connect)
if db_connect:
    spark = DatabricksSession.builder.profile("DEFAULT").getOrCreate()
else:
    spark = SparkSession.builder.getOrCreate()

FILE = f"/src/parquet_nested.parquet"
df = spark.read.parquet(INPUT_FILE_PATH)
df.show(truncate=False)
