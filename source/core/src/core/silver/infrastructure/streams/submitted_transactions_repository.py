from pyspark.sql import DataFrame, SparkSession

from core.settings.catalog_settings import CatalogSettings
from core.silver.domain.constants.col_names_silver_measurements import SilverMeasurementsColNames
from core.silver.infrastructure.config import SilverTableNames


class SubmittedTransactionsRepository:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.silver_database_name = CatalogSettings().silver_database_name  # type: ignore

    def read_submitted_transactions(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .table(f"{self.silver_database_name}.{SilverTableNames.silver_measurements}")
            .filter(
                f"{SilverMeasurementsColNames.orchestration_type} = 'OT_SUBMITTED_MEASURE_DATA'"
            )  # TODO: Add this to constants
        )
