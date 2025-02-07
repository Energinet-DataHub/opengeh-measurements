from pyspark.sql import DataFrame, SparkSession

from core.bronze.domain.constants import BronzeTableNames
from core.settings.catalog_settings import CatalogSettings


class BronzeRepository:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bronze_database_name = CatalogSettings().bronze_database_name  # type: ignore

    def read_measurements(self) -> DataFrame:
        return self.spark.readStream.format("delta").table(
            f"{self.bronze_database_name}.{BronzeTableNames.bronze_measurements_table}"
        )

    def read_submitted_transactions(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .table(f"{self.bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_table}")
        )
