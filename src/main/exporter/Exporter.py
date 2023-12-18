from abc import ABC

from pyspark.sql import DataFrame

from config.jsonConfig.exportConfig import ExportConfig


class Exporter(ABC):
    def __init__(self, input_dataset: DataFrame, export_config: ExportConfig):
        self.input_dataset = input_dataset
        self.export_config = export_config

    def export(self) -> None:
        pass
