from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from datetime import datetime

from opengeh_bronze.domain.constants.database_names import DatabaseNames
from opengeh_bronze.domain.constants.table_names import TableNames, MigrationsTableNames
import opengeh_bronze.domain.constants.column_names.bronze_migrated_column_names as BronzeMigratedColumnNames


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
        return data.write.mode("append").saveAsTable(
            f"{target_database}.{target_table_name}"
        )

    def calculate_latest_created_timestamp_that_has_been_migrated(self) -> datetime:
        return (
            self.read_measurements_bronze_migrated()
            .agg(max(col(BronzeMigratedColumnNames.created)))
            .collect()[0][0]
        )
