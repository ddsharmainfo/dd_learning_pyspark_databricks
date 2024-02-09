# from src_databricks_python_modules import get_spark_session
from src_databricks_python_modules.get_spark_session import get_session


def get_range_df(spark_session):
    print('from module1 get_range_df')
    df = spark_session.range(5)
    return df


if __name__ == "__main__":
    print('module1 run_range_df started')
    # spark = get_spark_session.get_session()
    spark = get_session()
    df = get_range_df(spark)
    df.show()
    print('module1 run_range_df ended')