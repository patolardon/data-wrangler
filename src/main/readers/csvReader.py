from pyspark.sql import DataFrame

from readers.reader import Reader


class CsvReader(Reader):
    def read(self) -> DataFrame:
        return self.spark.read.options(**self.options).csv(self.path)

