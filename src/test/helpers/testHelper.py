from pyspark.sql import DataFrame

from config.config import Config
from pipeline.pipeline import Pipeline
from readers.tableReader import TableReader


class TestHelperError(Exception):
    pass


class TestHelper:
    def __init__(self, spark, directory):
        self.spark = spark
        self.directory = directory
        self.config = Config(env="test", config_path="../resources", destination="db_name.table_name")

    def read_target(self, table) -> DataFrame:
        return self.spark.read.option("multiline", True).json(f"dataResources/{self.directory}/out/{table}.json")

    def create_source(self, table: str) -> None:
        source = self.spark.read.option("multiline", True).json(f"dataResources/{self.directory}/in/{table}.json")
        source.createOrReplaceTempView(f"{table}")

    def prepare_pipeline(self, pipeline: type(Pipeline)) -> Pipeline:
        for table in self.config.source_config.sources:
            if table.reader is TableReader:
                self.create_source(table.path)
        integration_pipeline = pipeline(self.spark, config=self.config)
        print(integration_pipeline)
        return integration_pipeline

    def apply_test(self, pipeline: type(Pipeline), target_name="target"):
        data = self.prepare_pipeline(pipeline).apply()
        target = self.read_target(target_name)
        return (data.exceptAll(target.select(data.columns)).persist().isEmpty() and
                target.exceptAll(data.select(target.columns)).persist().isEmpty())

    def export_test(self, pipeline: type(Pipeline)) -> bool:
        destination_splitted = self.config.export_config.destination.split(".")
        if len(destination_splitted) > 2:
            raise TestHelperError(f"destination_path must have one . separator maximum, night now it has "
                                  f"{len(destination_splitted)}")
        elif len(destination_splitted) > 1:
            db_name = destination_splitted[0]
            self.spark.sql(f"create database {db_name}")
        pipeline = self.prepare_pipeline(pipeline)
        pipeline.exporter.export()
        self.spark.sql("show tables").show()
        if len(destination_splitted) > 1:
            db_name = destination_splitted[0]
            self.spark.sql(f"show tables in {db_name}").show()
        target = self.spark.read.csv(self.config.export_config.destination_path)
        empty_target = ~(target.isEmpty())
        return empty_target
