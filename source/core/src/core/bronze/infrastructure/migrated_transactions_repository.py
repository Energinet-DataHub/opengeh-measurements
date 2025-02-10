from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from core.bronze.domain.constants.column_names.bronze_migrated_column_names import BronzeMigratedColumnNames
from core.bronze.infrastructure.config.table_names import TableNames
from core.migrations.database_names import DatabaseNames


class MigratedTransactionsRepository:
    def __init__(
        self,
        spark: SparkSession,
    ) -> None:
        self._spark = spark

    def read_measurements_bronze_migrated(self) -> DataFrame:
        target_database = DatabaseNames.bronze_database
        target_table_name = TableNames.bronze_migrated_transactions_table
        return self._spark.read.table(f"{target_database}.{target_table_name}")

    def write_measurements_bronze_migrated(self, data: DataFrame) -> None:
        target_database = DatabaseNames.bronze_database
        target_table_name = TableNames.bronze_migrated_transactions_table
        return data.write.mode("append").saveAsTable(f"{target_database}.{target_table_name}")

    def calculate_latest_created_timestamp_that_has_been_migrated(self) -> datetime:
        return (
            self.read_measurements_bronze_migrated()
            .agg(F.max(F.col(BronzeMigratedColumnNames.created_in_migrations)))
            .collect()[0][0]
        )
