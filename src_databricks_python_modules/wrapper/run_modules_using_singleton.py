from src_databricks_python_modules.get_spark_session_singleton import SparkSessionSingleton
from src_databricks_python_modules.module1 import run_range_df


def get_range_df(spark_session):
    print('from run_modules_using_singleton get_range_df')
    df = spark_session.range(2)
    return df


if __name__ == "__main__":
    print('main run_modules_using_singleton started')

    spark = SparkSessionSingleton.get_spark_session()

    df1 = get_range_df(spark)
    df1.show()

    df2 = run_range_df.get_range_df(spark)
    df2.show()
    
    print('main run_modules_using_singleton ended')