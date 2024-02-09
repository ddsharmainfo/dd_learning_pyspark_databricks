from src_databricks_python_modules.get_spark_session_singleton import SparkSessionSingleton get_spark_session
# from src_databricks_python_modules.module1 import run_range_df
# from src_databricks_python_modules.module2.run_range_df import get_range_df


def get_range_df(spark_session):
    print('from run_modules_using_singleton get_range_df')
    df = spark_session.range(2)
    return df


if __name__ == "__main__":
    print('run_range_df started')
    spark = SparkSessionSingleton.get_spark_session()
    df = get_range_df(spark)
    df.show()
    print('run_range_df ended')