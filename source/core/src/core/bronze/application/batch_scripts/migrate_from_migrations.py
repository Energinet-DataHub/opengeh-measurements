from datetime import datetime

from pyspark.sql.functions import col, lit

import core.bronze.application.config.spark_session as spark_session
import core.bronze.domain.transformations.migrate_from_migrations_transformations as migrate_from_migrations_transformations
from core.bronze.domain.constants.column_names.migrations_silver_time_series_column_names import (
    MigrationsSilverTimeSeriesColumnNames,
)
from core.bronze.infrastructure.repositories.migrated_transactions_repository import (
    MigratedTransactionsRepository,
)
from core.bronze.infrastructure.migration_data.silver_time_series_repository import (
    MigrationsSilverTimeSeriesRepository,
)


def migrate_time_series_from_migrations_to_measurements() -> None:
    spark = spark_session.initialize_spark()
    migrated_transactions_repository = MigratedTransactionsRepository(spark)
    migrations_silver_time_series_repository = MigrationsSilverTimeSeriesRepository(spark)

    latest_created_already_migrated = (
        migrated_transactions_repository.calculate_latest_created_timestamp_that_has_been_migrated()
    )
    # Determine which technique to apply for loading data.
    if latest_created_already_migrated is None:
        full_load_of_migrations_to_measurements(
            migrations_silver_time_series_repository, migrated_transactions_repository
        )
    else:
        daily_load_of_migrations_to_measurements(
            migrations_silver_time_series_repository,
            migrated_transactions_repository,
            latest_created_already_migrated,
        )


# Rely on the created column to identify what to migrate.
def daily_load_of_migrations_to_measurements(
    migrations_silver_time_series_repository: MigrationsSilverTimeSeriesRepository,
    migrated_transactions_repository: MigratedTransactionsRepository,
    latest_created_already_migrated: datetime,
) -> None:
    today = datetime.now().date()

    migrations_data = migrations_silver_time_series_repository.read_migrations_silver_time_series().filter(
        (col(MigrationsSilverTimeSeriesColumnNames.created) < lit(today))
        & (col(MigrationsSilverTimeSeriesColumnNames.created) > lit(latest_created_already_migrated))
    )

    migrations_data_transformed = migrate_from_migrations_transformations.map_migrations_to_measurements(
        migrations_data
    )

    migrated_transactions_repository.write_measurements_bronze_migrated(migrations_data_transformed)


# Leverage the transaction_insert_date partitioning to split our work into chunks due to the large amount of data to migrate.
def full_load_of_migrations_to_measurements(
    migrations_silver_time_series_repository: MigrationsSilverTimeSeriesRepository,
    migrated_transactions_repository: MigratedTransactionsRepository,
    num_chunks=10,
) -> None:
    migrations_data = migrations_silver_time_series_repository.read_migrations_silver_time_series()

    chunks = MigrationsSilverTimeSeriesRepository.create_chunks_of_partitions_for_data_with_a_single_partition_col(
        migrations_data,
        MigrationsSilverTimeSeriesColumnNames.partitioning_col,
        num_chunks,
    )
    today = datetime.now().date()

    for chunk in enumerate(chunks):
        migrations_data_chunk = migrations_silver_time_series_repository.read_migrations_silver_time_series().filter(
            (lit(chunk[0]) <= col(MigrationsSilverTimeSeriesColumnNames.partitioning_col))
            & (col(MigrationsSilverTimeSeriesColumnNames.partitioning_col) <= lit(chunk[-1]))
            & (col(MigrationsSilverTimeSeriesColumnNames.created) < lit(today))
        )

        migrations_data_transformed = migrate_from_migrations_transformations.map_migrations_to_measurements(
            migrations_data_chunk
        )

        migrated_transactions_repository.write_measurements_bronze_migrated(migrations_data_transformed)
