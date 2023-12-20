import pytest
from pyspark.sql import SparkSession
import shutil

from src.main.pipeline.impl.GpiPipeline import GpiPipeline
from src.main.pipeline.impl.fpaPipeline import FpaPipeline
from src.test.helpers.testHelper import TestHelper


@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Test Application").getOrCreate()
    yield spark


def test_pipeline(spark_fixture):
    assert TestHelper(spark_fixture, "tableReader").apply_test(FpaPipeline)
    assert TestHelper(spark_fixture, "csvReader").apply_test(GpiPipeline)


def test_export(spark_fixture):
    test_helper = TestHelper(spark_fixture, "tableReader")
    assert test_helper.export_test(FpaPipeline)
    try:
        print(test_helper.config.export_config.destination_path)
        shutil.rmtree(test_helper.config.export_config.destination_path)
        shutil.rmtree("spark-warehouse")
    except FileNotFoundError:
        pass



