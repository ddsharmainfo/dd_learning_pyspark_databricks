from pyspark import SparkConf
from pyspark.sql import SparkSession

def get_session():
    spark = SparkSession.builder.getOrCreate()
    return spark