from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import numpy as np

from opengeh_bronze.application.settings import KafkaAuthenticationSettings, SubmittedTransactionsStreamSettings
from opengeh_bronze.domain.constants.database_names import DatabaseNames
from opengeh_bronze.domain.constants.table_names import TableNames, MigrationsTableNames
import opengeh_bronze.domain.constants.column_names.bronze_migrated_column_names as BronzeMigratedColumnNames


# The source table in migrations is an append-only table that we want to copy 1-to-1 with no changes.
# As such, we can use the "created" column in order to keep track of progress in a daily manner.
# Ideally though, the data is partitioned by transaction_insert_date...
target_database = DatabaseNames.bronze_database
target_table_name = TableNames.bronze_migrated_transactions_table
fully_qualified_target_table_name = f"{target_database}.{target_table_name}"

source_database = DatabaseNames.silver_migrations_database
source_table_name = MigrationsTableNames.silver_time_series_table
fully_qualified_source_table_name = f"{source_database}.{source_table_name}"

today = datetime.now().date()

latest_created_already_migrated = datetime(1900, 1, 1).date()

try:
    latest_created_already_migrated = spark.read.table(fully_qualified_target_table_name).agg(F.max(F.col(BronzeMigratedColumnNames.created))).collect()[0][0]
except IndexError:
    print(f"{datetime.now()} - Failed to find any data in {fully_qualified_target_table_name}, assuming running from scratch.")


if latest_created_already_migrated == datetime(1900, 1, 1).date():
    # We can leverage the transaction_insert_date partitioning to split our work into chunks.
    NUM_CHUNKS = 10
    partitioning_column = "partitioning_col"
    partitions = sorted([row[partitioning_column] for row in spark.read.table(fully_qualified_source_table_name).select(partitioning_column).distinct().collect()])
    chunks = [chunk.tolist() for chunk in np.array_split(partitions, NUM_CHUNKS)]

    for i, chunk in enumerate(chunks):
        print(f"{datetime.now()} - Migrating partitions between: '{chunk[0]}' and '{chunk[-1]}', chunk {i}/{NUM_CHUNKS}")
        (
            spark
            .read
            .table(fully_qualified_source_table_name)
            .filter(f"'{chunk[0]}' <= {partitioning_column} and {partitioning_column} <= '{chunk[-1]}' and {BronzeMigratedColumnNames.created} < '{today}'")
            .write
            .mode("append")
            .saveAsTable(fully_qualified_target_table_name)
        )
else:
    # We can not leverage partitioning in the daily use-case, but the data is small enough to load naïvely.
    print(f"{datetime.now()} - Loading data written since {latest_created_already_migrated} into bronze.")
    (
        spark
        .read
        .table(fully_qualified_source_table_name)
        .filter((F.col(BronzeMigratedColumnNames.created) < F.lit(today)) & (F.col(BronzeMigratedColumnNames.created) > latest_created_already_migrated))
        .write
        .mode("append")
        .saveAsTable(fully_qualified_target_table_name)
    )

