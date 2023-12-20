from pyspark.sql import DataFrame
from pipeline.pipeline import Pipeline


class GpiPipeline(Pipeline):
    def apply(self) -> DataFrame:
        return self.source("gpi").groupBy("partner_id").count()

