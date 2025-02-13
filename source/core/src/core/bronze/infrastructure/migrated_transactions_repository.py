from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from core.bronze.domain.constants.column_names.bronze_migrated_column_names import BronzeMigratedColumnNames
from core.bronze.infrastructure.config.table_names import TableNames
from core.settings.catalog_settings import CatalogSettings


class MigratedTransactionsRepository:
    def __init__(
        self,
        spark: SparkSession,
    ) -> None:
        self._spark = spark
        self.bronze_database_name = CatalogSettings().bronze_database_name  # type: ignore
        self.migrated_table_name = TableNames.bronze_migrated_transactions_table

    def read_measurements_bronze_migrated(self) -> DataFrame:
        return self._spark.read.table(f"{self.bronze_database_name}.{self.migrated_table_name}")

    def write_measurements_bronze_migrated(self, data: DataFrame) -> None:
        return (
            data.write.format("delta")
            .option("mergeSchema", "false")
            .mode("append")
            .saveAsTable(f"{self.bronze_database_name}.{self.migrated_table_name}")
        )

    def calculate_latest_created_timestamp_that_has_been_migrated(self) -> datetime:
        return (
            self.read_measurements_bronze_migrated()
            .agg(F.max(F.col(BronzeMigratedColumnNames.created_in_migrations)))
            .collect()[0][0]
        )
