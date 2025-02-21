from pyspark.sql import DataFrame, SparkSession

from core.settings.catalog_settings import CatalogSettings
from core.silver.infrastructure.config import SilverTableNames


class SubmittedTransactionsRepository:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.silver_database_name = CatalogSettings().silver_database_name  # type: ignore

    def read_submitted_transactions(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .option("skipChangeCommits", "true")
            .table(f"{self.silver_database_name}.{SilverTableNames.silver_measurements}")
        )
