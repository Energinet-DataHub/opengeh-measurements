from pyspark.sql import DataFrame

import core.silver.infrastructure.config.spark as spark_session
from core.bronze.infrastructure.config import BronzeTableNames
from core.settings.catalog_settings import CatalogSettings


class BronzeRepository:
    def __init__(self):
        self.spark = spark_session.initialize_spark()
        self.bronze_database_name = CatalogSettings().bronze_database_name  # type: ignore

    def read_submitted_transactions(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .table(f"{self.bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_table}")
        )

    def read_calculated_measurements(self) -> DataFrame:
        raise NotImplementedError("Not implemented yet")
