from src_databricks_python_modules.get_spark_session_singleton import SparkSessionSingleton
from src_databricks_python_modules.config.configuration import file_path


def read_parquet(spark, file_path):
    df = spark.read.format('parquet').load(file_path).limit(100)
    return df


# if __name__ == "__main__":
#     spark = SparkSessionSingleton.get_spark_session()

#     df = read_parquet(file_path)
#     df.show()