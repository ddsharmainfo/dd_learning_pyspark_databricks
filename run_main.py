import parquet_read

from pyspark.sql import SparkSession

# Create or get spark session
spark = SparkSession.builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


if __name__ == '__main__':
    parquet_read.read_parquet(spark)