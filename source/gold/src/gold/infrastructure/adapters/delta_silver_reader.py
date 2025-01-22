from pyspark.sql import SparkSession, DataFrame

from gold.application.ports.silver_reader import SilverReader
from gold.infrastructure.config.delta_silver_config import DeltaSilverReaderConfig


class DeltaSilverReader(SilverReader):
    def __init__(self, spark: SparkSession, config: DeltaSilverReaderConfig):
        config.validate()
        self.spark = spark
        self.config = config

    def read(self) -> DataFrame:
        return (self.spark.readStream
                .format("delta")
                .options(**self.config.read_options)
                .table(self.config.full_silver_table_name))
