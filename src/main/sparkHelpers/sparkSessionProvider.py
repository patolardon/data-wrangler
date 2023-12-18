from pyspark.sql import SparkSession


def get_spark():
    return SparkSession.builder.master("local[1]") \
                        .appName('Pierre App') \
                        .getOrCreate()
