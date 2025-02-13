from pyspark.sql import DataFrame, SparkSession

from core.bronze.infrastructure.config.table_names import MigrationsTableNames
from core.migrations.database_names import DatabaseNames


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

    @staticmethod
    def create_chunks_of_partitions_for_data_with_a_single_partition_col(
        data, partition_col: str, num_chunks: int
    ) -> list[list[str]]:
        partitions = sorted([str(row[partition_col]) for row in data.select(partition_col).distinct().collect()])

        chunk_size = len(partitions) // num_chunks
        remainder = len(partitions) % num_chunks
        chunks = []
        start = 0

        for i in range(num_chunks):
            end = start + chunk_size + (1 if i < remainder else 0)
            chunks.append(partitions[start:end])
            start = end

        return [chunk for chunk in chunks if len(chunk) > 0]
