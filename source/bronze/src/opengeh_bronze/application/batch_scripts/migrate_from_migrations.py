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
import opengeh_bronze.domain.transformations.migrate_from_migrations_transformations as migrate_from_migrations_transformations
import opengeh_bronze.domain.constants.column_names.bronze_migrated_column_names as BronzeMigratedColumnNames
import opengeh_bronze.application.config.spark_session as spark_session


def migrate_from_migrations_to_measurements() -> None:
    spark = spark_session.initialize_spark()

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
                fully_qualified_target_table_name
            )
        )
    except IndexError:
        print(
            f"{datetime.now()} - Failed to find any data in {fully_qualified_target_table_name}, doing full load of migrations to measurements from scratch."
        )

    # Determine which technique to apply for loading data.
    if latest_created_already_migrated == datetime(1900, 1, 1).date():
        full_load_of_migrations_to_measurements(
            spark, fully_qualified_source_table_name, fully_qualified_target_table_name
        )
    else:
        daily_load_of_migrations_to_measurements(
            spark,
            fully_qualified_source_table_name,
            fully_qualified_target_table_name,
            latest_created_already_migrated,
        )


# Rely on the created column to identify what to migrate.
def daily_load_of_migrations_to_measurements(
    fully_qualified_source_table_name: str,
    fully_qualified_target_table_name: str,
    latest_created_already_migrated: datetime,
) -> None:
    today = datetime.now().date()
    print(
        f"{datetime.now()} - Loading data written since {latest_created_already_migrated} into bronze."
    )
    migrations_data = spark.read.table(fully_qualified_source_table_name).filter(
        (col(BronzeMigratedColumnNames.created) < lit(today))
        & (
            col(BronzeMigratedColumnNames.created)
            > lit(latest_created_already_migrated)
        )
    )

    append_to_measurements(migrations_data, fully_qualified_target_table_name)


# Leverage the transaction_insert_date partitioning to split our work into chunks due to the large amount of data to migrate.
def full_load_of_migrations_to_measurements(
    fully_qualified_source_table_name: str, fully_qualified_target_table_name: str
) -> None:
    NUM_CHUNKS = 10
    partitioning_column = "partitioning_col"
    chunks = migrate_from_migrations_transformations.create_chunks_of_partitions(
        fully_qualified_source_table_name, partitioning_column, NUM_CHUNKS
    )
    today = datetime.now().date()

    for i, chunk in enumerate(chunks):
        print(
            f"{datetime.now()} - Migrating partitions between: '{chunk[0]}' and '{chunk[-1]}', chunk {i}/{NUM_CHUNKS}"
        )

        migrations_data = spark.read.table(fully_qualified_source_table_name).filter(
            (lit(chunk[0]) <= col(partitioning_column))
            & (
                col(partitioning_column)
                <= lit(chunk[-1])
                & (col(BronzeMigratedColumnNames.created) < lit(today))
            )
        )

        append_to_measurements(migrations_data, fully_qualified_target_table_name)

def append_to_measurements(
    data: DataFrame, fully_qualified_target_table_name: str
) -> None:
    (data.write.mode("append").saveAsTable(fully_qualified_target_table_name))
