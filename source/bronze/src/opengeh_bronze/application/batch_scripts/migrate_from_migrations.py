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
import opengeh_bronze.domain.transformations.migrate_from_migrations_transformations as migrate_from_migrations_transformations
import opengeh_bronze.domain.constants.column_names.bronze_migrated_column_names as BronzeMigratedColumnNames
import opengeh_bronze.application.config.spark_session as spark_session


def migrate_from_migrations_to_measurements() -> None:
    spark = spark_session.initialize_spark()
    repository = Repository(spark)

    target_database = DatabaseNames.bronze_database
    target_table_name = TableNames.bronze_migrated_transactions_table
    fully_qualified_target_table_name = f"{target_database}.{target_table_name}"

    source_database = DatabaseNames.silver_migrations_database
    source_table_name = MigrationsTableNames.silver_time_series_table
    fully_qualified_source_table_name = f"{source_database}.{source_table_name}"

    latest_created_already_migrated = datetime(1900, 1, 1).date()

    try:
        latest_created_already_migrated = (
            migrate_from_migrations_transformations.calculate_latest_created_timestamp_that_has_been_migrated(
                repository
            )
        )
    except IndexError:
        print(
            f"{datetime.now()} - Failed to find any data in {fully_qualified_target_table_name}, doing full load of migrations to measurements from scratch."
        )

    # Determine which technique to apply for loading data.
    if latest_created_already_migrated == datetime(1900, 1, 1).date():
        full_load_of_migrations_to_measurements(
            spark, repository
        )
    else:
        daily_load_of_migrations_to_measurements(
            spark,
            repository,
            latest_created_already_migrated,
        )


# Rely on the created column to identify what to migrate.
def daily_load_of_migrations_to_measurements(
    repository: Repository,
    latest_created_already_migrated: datetime,
) -> None:
    today = datetime.now().date()
    print(
        f"{datetime.now()} - Loading data written since {latest_created_already_migrated} into bronze."
    )
    migrations_data = repository.read_migrations_silver_time_series().filter(
        (col(BronzeMigratedColumnNames.created) < lit(today))
        & (
            col(BronzeMigratedColumnNames.created)
            > lit(latest_created_already_migrated)
        )
    )

    repository.write_measurements_bronze_migrated(migrations_data)


# Leverage the transaction_insert_date partitioning to split our work into chunks due to the large amount of data to migrate.
def full_load_of_migrations_to_measurements(
    repository: Repository
) -> None:
    NUM_CHUNKS = 10
    partitioning_column = "partitioning_col"
    chunks = migrate_from_migrations_transformations.create_chunks_of_partitions(
        repository, partitioning_column, NUM_CHUNKS
    )
    today = datetime.now().date()

    for i, chunk in enumerate(chunks):
        print(
            f"{datetime.now()} - Migrating partitions between: '{chunk[0]}' and '{chunk[-1]}', chunk {i}/{NUM_CHUNKS}"
        )

        migrations_data = repository.read_migrations_silver_time_series().filter(
            (lit(chunk[0]) <= col(partitioning_column))
            & (
                col(partitioning_column)
                <= lit(chunk[-1])
                & (col(BronzeMigratedColumnNames.created) < lit(today))
            )
        )

        repository.write_measurements_bronze_migrated(migrations_data)
