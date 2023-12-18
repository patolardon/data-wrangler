from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class Reader(ABC):
    def __init__(self, spark: SparkSession, path: str, options: dict):
        self.spark = spark
        self.path = path
        self.options = options

    @abstractmethod
    def read(self, **kwargs) -> DataFrame:
        pass

    def __repr__(self):
        return f"{type(self).__name__} reading {self.path}"
