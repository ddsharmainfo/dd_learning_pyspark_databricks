from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


class SparkSessionSingleton:
    _spark = None
    _dbutils = None

    @staticmethod
    def get_spark_session():
        if SparkSessionSingleton._spark is None:
            SparkSessionSingleton()
        return SparkSessionSingleton._spark
    
    @staticmethod
    def get_dbutils():
        if SparkSessionSingleton._dbutils is None:
            SparkSessionSingleton()
        return SparkSessionSingleton._dbutils
    
    def __init__(self):
        if SparkSessionSingleton._spark is not None:
            raise Exception("SparkSessionSingleton already instantiated!")
        
        SparkSessionSingleton._spark = SparkSession.builder \
            .appName("Singleton Spark Session") \
            .getOrCreate()
        
        SparkSessionSingleton._dbutils = DBUtils(SparkSessionSingleton._spark)