from src import get_spark
from pyspark.sql.functions import col,current_date, months_between, add_months, date_add, date_sub, datediff,dayofweek,dayofyear,dayofmonth


data = [
    (1000, 'login', '2023-06-16 01:00:15.34'),
    (1000, 'login', '2023-06-16 02:00:15.34'),
    (1000, 'login', '2023-06-16 03:00:15.34'),
    (1000, 'logout', '2023-06-16 12:00:15.34'),
    (1001, 'login', '2023-06-16 01:00:15.34'),
    (1001, 'login', '2023-06-16 02:00:15.34'),
    (1001, 'login', '2023-06-16 03:00:15.34'),
    (1001, 'logout', '2023-06-16 12:00:15.34')
]

schema = ["employee_id","entry_details","time_stamp"]


if __name__ == '__main__':
    spark, sc = get_spark.init_spark()
    df = spark.createDataFrame(data=data, schema=schema)
    df = df.select(col("time_stamp"),months_between(current_date(),col("time_stamp")).alias("months_between"),\
             add_months(col("time_stamp"),5).alias("add_months"),\
             add_months(col("time_stamp"),-5).alias("sub_months"),\
             date_add(col("time_stamp"),3).alias("date_add"),\
             date_sub(col("time_stamp"),3).alias("date_sub"),\
             datediff(current_date(),col("time_stamp")).alias("datediff"),\
             dayofweek(col("time_stamp")).alias("dayofweek"),\
             dayofmonth(col("time_stamp")).alias("dayofmonth"),\
             dayofyear(col("time_stamp")).alias("dayofyear")
                  )
    df.show(truncate=False)