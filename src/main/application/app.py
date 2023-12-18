import argparse
import shutil

from pyspark.sql.utils import AnalysisException

from config.config import Config
from pipeline.pipeline import Pipeline
from sparkHelpers.sparkSessionProvider import get_spark


class ApplicationError(Exception):
    pass


class Application:
    def __init__(self, pipeline: type(Pipeline)):
        self.pipeline = pipeline

    @staticmethod
    def clean_after():
        try:
            print("cleaning spark-warehouse")
            shutil.rmtree("spark-warehouse")
        except FileNotFoundError:
            pass

    def run(self) -> None:
        arg_parser = argparse.ArgumentParser(
            description='''Pyspark script''')
        arg_parser.add_argument('--env',
                                help='''env to run in''',
                                required=False,
                                type=str,
                                nargs='?',
                                default="test")
        arg_parser.add_argument('--db_name',
                                help='''table to run in''',
                                required=False)
        arg_parser.add_argument('--clean-after',
                                help='''clean spark-warehouse''',
                                nargs='?',
                                type=bool,
                                required=False,
                                default=True)
        args = arg_parser.parse_args()
        env = args.env
        print(env)
        config = Config(env, "")
        spark = get_spark()
        try:
            pipeline = self.pipeline(spark, config)
            pipeline.apply().show()
        except AnalysisException as e:
            raise(ApplicationError(e))
        finally:
            if args.clean_after:
                self.clean_after()
