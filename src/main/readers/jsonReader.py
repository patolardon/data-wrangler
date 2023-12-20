from pyspark.sql import DataFrame

from readers.reader import Reader


class JsonReader(Reader):
    def read(self, **kwargs) -> DataFrame:
        return self.spark.read.options(**kwargs).json(self.path)
