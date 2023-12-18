from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from config.config import Config
import inspect

from exporter.AddExporter import AddExporter
from exporter.Exporter import Exporter


class ExportConfigError(Exception):
    pass


class Pipeline(ABC):
    def __init__(self, spark, config: Config):
        self.spark = spark
        self.config = config

    def __repr__(self):
        return f'Pipeline to apply transformations for {inspect.getsource(self.apply)}'

    def source(self, source_name: str) -> DataFrame:
        return {
            source.name: source.reader(self.spark, source.path, source.reader_options).read()
            for source in self.config.source_config.sources
        }.get(source_name)

    @property
    def exporter(self) -> Exporter:
        export_type = self.config.export_config.export_type
        try:
            return {"add": AddExporter(input_dataset=self.apply(),
                                       export_config=self.config.export_config)}[export_type]
        except KeyError:
            raise ExportConfigError(f"reader type {export_type} is not a valid key")

    @abstractmethod
    def apply(self) -> DataFrame:
        pass


