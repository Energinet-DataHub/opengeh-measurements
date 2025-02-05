from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, max
from datetime import datetime
import numpy as np

from opengeh_bronze.application.settings import (
    KafkaAuthenticationSettings,
    SubmittedTransactionsStreamSettings,
)
from opengeh_bronze.domain.constants.database_names import DatabaseNames
from opengeh_bronze.domain.constants.table_names import TableNames, MigrationsTableNames
from opengeh_bronze.infrastructure.repository import Repository
import opengeh_bronze.domain.constants.column_names.bronze_migrated_column_names as BronzeMigratedColumnNames
import opengeh_bronze.application.config.spark_session as spark_session



def calculate_latest_created_timestamp_that_has_been_migrated(
    repository: Repository,
) -> datetime:
    return (
        repository.read_migrations_silver_time_series()
        .agg(max(col(BronzeMigratedColumnNames.created)))
        .collect()[0][0]
    )


def create_chunks_of_partitions(
    repository: Repository, partition_col: str, num_chunks: int
) -> list[str]:
    partitions = sorted(
        [
            str(row[partition_col])
            for row in repository.read_migrations_silver_time_series()
            .select(partition_col)
            .distinct()
            .collect()
        ]
    )
    return [chunk.tolist() for chunk in np.array_split(partitions, num_chunks)]
