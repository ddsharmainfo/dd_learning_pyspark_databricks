from src import get_spark

input_path = '../input/country.csv'


if __name__ == "__main__":
    app_name = 'PySpark App Spark SQL'
    spark, sc = get_spark.get_spark_session(app_name)

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)

    df.createOrReplaceTempView("country_view")
    group_df = spark.sql("select Country, count(1) as count from country_view where Age<40 group by Country")
    group_df.show()