#from poc_python_modules import get_session
from poc_python_modules_with_dbx.get_session import get_spark_session

if __name__ == "__main__":
    print('main started')
    #spark = get_session.get_spark_session()
    spark = get_spark_session()
    spark.range(10).show()
    print('main end')
