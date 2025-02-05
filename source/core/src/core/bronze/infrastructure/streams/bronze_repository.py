from pyspark.sql import DataFrame, SparkSession

from core.bronze.domain.constants import BronzeDatabaseNames
from core.bronze.domain.constants import BronzeTableNames


class BronzeRepository:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_measurements(self) -> DataFrame:
        return self.spark.readStream.format("delta").table(
            f"{BronzeDatabaseNames.bronze_database}.{BronzeTableNames.bronze_measurements_table}"
        )

    def read_submitted_transactions(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .table(f"{BronzeDatabaseNames.bronze_database}.{BronzeTableNames.bronze_submitted_transactions_table}")
        )
