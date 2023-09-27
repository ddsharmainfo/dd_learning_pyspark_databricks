from src import get_spark
from pyspark.sql.functions import spark_partition_id

input_path = './../input/'
output_path = './../output/json/'
output_path2 = './../output/json2/'

if __name__ == "__main__":
    app_name = 'PySpark App Write JSON Sink'
    spark, sc = get_spark.get_spark_session(app_name)

    flight_parquet_df = spark.read \
        .format("parquet") \
        .load(input_path + "flight*.parquet")

    flight_parquet_df.show()
    flight_parquet_df.groupBy(spark_partition_id()).count().show()

    partitioned_df = flight_parquet_df.repartition(1)
    partitioned_df.groupBy(spark_partition_id()).count().show()

    partitioned_df.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", output_path) \
        .save()

    partitioned_df.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", output_path2) \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 1000) \
        .save()