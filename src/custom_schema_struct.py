from src import get_spark
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

input_path = '../input/'

if __name__ == "__main__":
    app_name = 'PySpark App Custom Schema Struct'
    spark, sc = get_spark.get_spark_session(app_name)

    schema_struct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flight_csv_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema_struct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load(input_path + "flight*.csv")

    flight_csv_df.show(5)
