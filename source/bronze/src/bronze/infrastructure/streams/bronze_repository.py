from pyspark.sql import DataFrame, SparkSession

from bronze.infrastructure.config.database_names import DatabaseNames
from bronze.infrastructure.config.table_names import TableNames


class BronzeRepository:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_measurements(self) -> DataFrame:
        return self.spark.readStream.format("delta").table(
            f"{DatabaseNames.bronze_database}.{TableNames.bronze_measurements_table}"
        )
