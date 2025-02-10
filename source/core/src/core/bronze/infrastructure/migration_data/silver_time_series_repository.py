import numpy as np
from pyspark.sql import DataFrame, SparkSession

from core.bronze.domain.constants.database_names import DatabaseNames
from core.bronze.domain.constants.table_names import MigrationsTableNames


class MigrationsSilverTimeSeriesRepository:
    def __init__(
        self,
        spark: SparkSession,
    ) -> None:
        self._spark = spark

    def read_migrations_silver_time_series(self) -> DataFrame:
        source_database = DatabaseNames.silver_migrations_database
        source_table_name = MigrationsTableNames.silver_time_series_table
        return self._spark.read.table(f"{source_database}.{source_table_name}")

    def create_chunks_of_migrations_partitions(self, partition_col: str, num_chunks: int) -> list[str]:
        partitions = sorted(
            [
                str(row[partition_col])
                for row in self.read_migrations_silver_time_series().select(partition_col).distinct().collect()
            ]
        )
        return [chunk.tolist() for chunk in np.array_split(partitions, num_chunks)]
