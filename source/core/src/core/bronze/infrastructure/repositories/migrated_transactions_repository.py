from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from core.bronze.domain.constants.column_names.bronze_migrated_transactions_column_names import (
    BronzeMigratedTransactionsColumnNames,
)
from core.bronze.infrastructure.config.table_names import TableNames
from core.settings.bronze_settings import BronzeSettings


class MigratedTransactionsRepository:
    def __init__(
        self,
        spark: SparkSession,
    ) -> None:
        self.spark = spark
        self.bronze_database_name = BronzeSettings().bronze_database_name
        self.migrated_transactions_table_name = TableNames.bronze_migrated_transactions_table

    def read_measurements_bronze_migrated_transactions(self) -> DataFrame:
        return self.spark.read.table(f"{self.bronze_database_name}.{self.migrated_transactions_table_name}")

    def write_measurements_bronze_migrated(self, data: DataFrame) -> None:
        return (
            data.write.format("delta")
            .option("mergeSchema", "false")
            .mode("append")
            .saveAsTable(f"{self.bronze_database_name}.{self.migrated_transactions_table_name}")
        )

    def calculate_latest_created_timestamp_that_has_been_migrated(self) -> Optional[datetime]:
        return (
            self.read_measurements_bronze_migrated_transactions()
            .agg(F.max(F.col(BronzeMigratedTransactionsColumnNames.created_in_migrations)))
            .collect()[0][0]
        )
