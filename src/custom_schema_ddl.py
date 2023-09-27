from src import get_spark
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

input_path = '../input/'

if __name__ == "__main__":
    app_name = 'PySpark App Custom Schema DDL'
    spark, sc = get_spark.get_spark_session(app_name)

    schema_ddl = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    flight_json_df = spark.read \
        .format("json") \
        .schema(schema_ddl) \
        .option("dateFormat", "M/d/y") \
        .load(input_path + "flight*.json")

    flight_json_df.show(5)

    flight_parquet_df = spark.read \
        .format("parquet") \
        .load(input_path + "flight*.parquet")

    flight_parquet_df.show(5)
