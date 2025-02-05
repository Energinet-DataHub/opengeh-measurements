from pyspark.sql import DataFrame, SparkSession

from opengeh_bronze.domain.constants.table_names import TableNames
from opengeh_bronze.infrastructure.settings import BronzeDatabaseSettings


class BronzeRepository:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bronze_database_settings = BronzeDatabaseSettings()  # type: ignore

    def read_measurements(self) -> DataFrame:
        return self.spark.readStream.format("delta").table(
            f"{self.bronze_database_settings.bronze_database_name}.{TableNames.bronze_measurements_table}"
        )

    def read_submitted_transactions(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .table(
                f"{self.bronze_database_settings.bronze_database_name}.{TableNames.bronze_submitted_transactions_table}"
            )
        )
