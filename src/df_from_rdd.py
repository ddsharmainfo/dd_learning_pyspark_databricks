# Create DF from Existing RDD:

from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession.builder.appName("RDDtoDF").master("local[*]").getOrCreate()

    data = [(1, "A"), (2, "B"), (3, "C")]
    schema = ["id", "Name"]

    rdd = spark.sparkContext.parallelize(data)
    df = spark.createDataFrame(rdd, schema)
    df.show()

    spark.stop()
