from pyspark.sql import DataFrame

from readers.reader import Reader


class TableReader(Reader):
    def read(self) -> DataFrame:
        return self.spark.read.options(**self.options).table(self.path)
