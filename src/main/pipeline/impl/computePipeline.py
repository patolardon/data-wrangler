from pyspark.sql import DataFrame

from pipeline.impl.GpiPipeline import GpiPipeline
from pipeline.impl.fpaPipeline import FpaPipeline
from pipeline.pipeline import Pipeline


class ComputePipeline(Pipeline):
    def apply(self) -> DataFrame:
        return FpaPipeline(self.spark, self.config).apply().union(GpiPipeline(self.spark, self.config).apply())