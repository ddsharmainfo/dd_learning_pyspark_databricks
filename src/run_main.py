from src import get_spark
from src import read_parquet

from pyspark.sql import SparkSession

# Create or get spark session
# spark = SparkSession.builder \
#     .appName("Python Spark SQL basic example") \
#     .master("local[*]") \
#     .getOrCreate()
#     #.config("spark.some.config.option", "some-value")


if __name__ == '__main__':
    spark, sc = get_spark.get_spark_session()
    print('----- Main Application Started -----')

    read_parquet.read_parquet(spark)

    print('----- Main Application Ended -----')