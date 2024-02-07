#from poc_python_modules import get_session
from poc_python_modules_with_dbx.get_session import get_spark_session


def print_range(session):
    df = session.range(5)
    return df


if __name__ == "__main__":
    print('main started')
    #spark = get_session.get_spark_session()
    spark = get_spark_session()
    df = print_range(spark)
    df.show()
    print('main end')
