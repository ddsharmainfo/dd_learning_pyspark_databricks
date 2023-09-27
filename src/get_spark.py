from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session(app_name):
    if app_name == '':
        app_name = 'DD Learning PySpark'
        configs = SparkConf().setAppName(app_name)
    else:
        configs = SparkConf().setAppName(app_name)

    spark = SparkSession.builder \
        .master("local[*]") \
        .config(conf=configs) \
        .getOrCreate()
        #.appName("DD Learning PySpark") \
        #.config("spark.some.config.option", "some-value") \
    sc = spark.sparkContext
    print('Application Name: ', spark.conf.get('spark.app.name'))
    return spark, sc