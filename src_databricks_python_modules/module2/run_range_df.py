# from src_databricks_python_modules import get_spark_session
from src_databricks_python_modules.get_spark_session import get_session


def get_range_df(spark_session):
    print('from module2 get_range_df')
    df = spark_session.range(10)
    return df


if __name__ == "__main__":
    print('module2 run_range_df started')
    # spark = get_session.get_spark_session()
    spark = get_session()
    df = get_range_df(spark)
    df.show()
    print('module2 run_range_df ended')