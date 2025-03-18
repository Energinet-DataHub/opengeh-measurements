from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from core.bronze.domain.constants.column_names.migrations_silver_time_series_column_names import (
    MigrationsSilverTimeSeriesColumnNames,
)
from core.bronze.infrastructure.config.table_names import MigrationsTableNames
from core.settings.migrations_settings import MigrationsSettings


class MigrationsSilverTimeSeriesRepository:
    def __init__(
        self,
        spark: SparkSession,
    ) -> None:
        self.spark = spark
        self.source_database = MigrationsSettings().silver_database_name
        self.source_table_name = MigrationsTableNames.silver_time_series_table
        self.repo_creation_timestamp = datetime.now()

    def read_migrations_silver_time_series(self) -> DataFrame:
        return self.spark.read.table(f"{self.source_database}.{self.source_table_name}")

    def read_migrations_silver_time_series_written_since_timestamp(
        self, latest_created_already_migrated: datetime
    ) -> DataFrame:
        return self.read_migrations_silver_time_series().filter(
            (col(MigrationsSilverTimeSeriesColumnNames.created) < self.repo_creation_timestamp)
            & (col(MigrationsSilverTimeSeriesColumnNames.created) > lit(latest_created_already_migrated))
        )

    def read_chunk_of_migrations_silver_time_series_partitions(
        self, start_partition_date, end_partition_date
    ) -> DataFrame:
        return self.read_migrations_silver_time_series().filter(
            (lit(start_partition_date) <= col(MigrationsSilverTimeSeriesColumnNames.partitioning_col))
            & (col(MigrationsSilverTimeSeriesColumnNames.partitioning_col) <= lit(end_partition_date))
            & (col(MigrationsSilverTimeSeriesColumnNames.created) < self.repo_creation_timestamp)
        )

    def create_chunks_of_partitions_for_data_with_a_single_partition_col(
        self, partition_col: str, num_chunks: int
    ) -> list[list[str]]:
        migrations_data = self.read_migrations_silver_time_series()
        partitions = sorted(
            [str(row[partition_col]) for row in migrations_data.select(partition_col).distinct().collect()]
        )

        chunk_size = len(partitions) // num_chunks
        remainder = len(partitions) % num_chunks
        chunks = []
        start = 0

        for i in range(num_chunks):
            end = start + chunk_size + (1 if i < remainder else 0)
            chunks.append(partitions[start:end])
            start = end

        return [chunk for chunk in chunks if len(chunk) > 0]
