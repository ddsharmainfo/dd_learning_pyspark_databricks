from src_databricks_python_modules.get_spark_session_singleton import SparkSessionSingleton
from src_databricks_python_modules.config.configuration import file_path
from src_databricks_python_modules.data_pre_processing.read_data import read_parquet
from pyspark.sql.functions import col


def get_new_product_id(spark, df):
    df = df.withColumn('new_product_id', col('product_id')+1)
    return df


# if __name__ == "__main__":
#     spark = SparkSessionSingleton.get_spark_session()
#     df      = read_parquet(spark, file_path)
#     df_new  = get_new_product_id(spark, df)
#     df_new.show()