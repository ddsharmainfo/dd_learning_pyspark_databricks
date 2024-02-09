from src_databricks_python_modules.get_spark_session import get_session
from src_databricks_python_modules.module1 import run_range_df
from src_databricks_python_modules.module2.run_range_df import get_range_df


if __name__ == "__main__":
    print('run_modules main started')
    spark = get_session()
    df1 = run_range_df.get_range_df(spark)
    df1.show()

    df2 = get_range_df(spark)
    df2.show()
    print('run_modules main ended')