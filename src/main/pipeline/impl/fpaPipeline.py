from pyspark.sql import DataFrame
from pipeline.pipeline import Pipeline
from transformers.transformers import select_pipeline, groupby_pipeline
from functools import partial


class FpaPipeline(Pipeline):
    def apply(self) -> DataFrame:
        return (self.source("fpa")
                .transform(select_pipeline)
                .transform(partial(groupby_pipeline, key="dt_rcp"))
                )
