from src import get_spark

input_path = '../input/country.csv'


if __name__ == "__main__":
    app_name = 'PySpark App Read CSV'
    spark, sc = get_spark.get_spark_session(app_name)

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)

    df.show()

    df = df.filter("Age < 40").select("Age", "Gender", "Country", "state")

    df.show()

    df = df.groupBy("Country").count()

    df.show()

    #spark.stop()