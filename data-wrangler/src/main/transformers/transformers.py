from pyspark.sql.functions import *


def select_pipeline(data: DataFrame):
    return (data
            .filter(col("dt_rcp") == "2023-12-12")
            .select("dt_rcp", "nu_cli_mrk"))


def groupby_pipeline(data: DataFrame, key):
    return (data
            .groupby(key)
            .count())

