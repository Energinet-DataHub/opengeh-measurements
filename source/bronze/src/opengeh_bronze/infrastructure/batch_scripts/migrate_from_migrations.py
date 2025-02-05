from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import numpy as np

from opengeh_bronze.application.settings import (
    KafkaAuthenticationSettings,
    SubmittedTransactionsStreamSettings,
)
from opengeh_bronze.domain.constants.database_names import DatabaseNames
from opengeh_bronze.domain.constants.table_names import TableNames, MigrationsTableNames
import opengeh_bronze.domain.constants.column_names.bronze_migrated_column_names as BronzeMigratedColumnNames
import opengeh_bronze.application.config.spark_session as spark_session


def migrations_to_measurements():
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
            spark.read.table(fully_qualified_target_table_name)
            .agg(F.max(F.col(BronzeMigratedColumnNames.created)))
            .collect()[0][0]
        )
    except IndexError:
        print(
            f"{datetime.now()} - Failed to find any data in {fully_qualified_target_table_name}, assuming running from scratch."
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
    fully_qualified_source_table_name,
    fully_qualified_target_table_name,
    latest_created_already_migrated,
):
    today = datetime.now().date()
    print(
        f"{datetime.now()} - Loading data written since {latest_created_already_migrated} into bronze."
    )
    (
        spark.read.table(fully_qualified_source_table_name)
        .filter(
            (F.col(BronzeMigratedColumnNames.created) < F.lit(today))
            & (
                F.col(BronzeMigratedColumnNames.created)
                > latest_created_already_migrated
            )
        )
        .write.mode("append")
        .saveAsTable(fully_qualified_target_table_name)
    )


# Leverage the transaction_insert_date partitioning to split our work into chunks due to the large amount of data to migrate.
def full_load_of_migrations_to_measurements(
    fully_qualified_source_table_name, fully_qualified_target_table_name
):
    NUM_CHUNKS = 10
    partitioning_column = "partitioning_col"
    partitions = sorted(
        [
            row[partitioning_column]
            for row in spark.read.table(fully_qualified_source_table_name)
            .select(partitioning_column)
            .distinct()
            .collect()
        ]
    )
    chunks = [chunk.tolist() for chunk in np.array_split(partitions, NUM_CHUNKS)]
    today = datetime.now().date()

    for i, chunk in enumerate(chunks):
        print(
            f"{datetime.now()} - Migrating partitions between: '{chunk[0]}' and '{chunk[-1]}', chunk {i}/{NUM_CHUNKS}"
        )
        (
            spark.read.table(fully_qualified_source_table_name)
            .filter(
                f"'{chunk[0]}' <= {partitioning_column} and {partitioning_column} <= '{chunk[-1]}' and {BronzeMigratedColumnNames.created} < '{today}'"
            )
            .write.mode("append")
            .saveAsTable(fully_qualified_target_table_name)
        )
