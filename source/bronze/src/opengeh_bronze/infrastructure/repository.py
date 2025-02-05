from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from opengeh_bronze.domain.constants.database_names import DatabaseNames
from opengeh_bronze.domain.constants.table_names import TableNames, MigrationsTableNames

class Repository:
    def __init__(
        self,
        spark: SparkSession,
    ) -> None:
        self._spark = spark

    def read_migrations_silver_time_series(self) -> DataFrame:
        source_database = DatabaseNames.silver_migrations_database
        source_table_name = MigrationsTableNames.silver_time_series_table
        return self._spark.read.table(f"{source_database}.{source_table_name}")


    def read_measurements_bronze_migrated(self) -> DataFrame:
        target_database = DatabaseNames.bronze_database
        target_table_name = TableNames.bronze_migrated_transactions_table
        return self._spark.read.table(f"{target_database}.{target_table_name}")


    def write_measurements_bronze_migrated(self, data: DataFrame) -> None:
        target_database = DatabaseNames.bronze_database
        target_table_name = TableNames.bronze_migrated_transactions_table
        return data.write.mode("append").saveAsTable(f"{target_database}.{target_table_name}")
