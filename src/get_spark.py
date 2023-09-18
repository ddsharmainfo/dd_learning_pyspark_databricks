from pyspark.sql import SparkSession


def init_spark():
    spark = SparkSession.builder \
        .appName("PySpark Application") \
        .master("local[*]") \
        .getOrCreate()
        #.config("spark.some.config.option", "some-value") \

    sc = spark.sparkContext
    return spark, sc