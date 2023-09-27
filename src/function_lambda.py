from src import get_spark


def main():
    spark, sc = get_spark.get_spark_session()
    nums = sc.parallelize([1, 2, 3, 4])
    print(nums.map(lambda x: x * x).collect())


if __name__ == '__main__':
    main()
