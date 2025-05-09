import datetime

from pyspark.sql import SparkSession
from pytest_bdd import given, parsers, scenarios, then, when

import core.bronze.application.batch_scripts.migrate_from_migrations as migrate_from_migrations
import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config import BronzeTableNames
from core.bronze.infrastructure.config.table_names import MigrationsTableNames as MigrationsSilverTableNames
from core.bronze.infrastructure.migration_data.silver_time_series_repository import (
    MigrationsSilverTimeSeriesRepository,
)
from core.bronze.infrastructure.repositories.migrated_transactions_repository import (
    MigratedTransactionsRepository,
)
from core.settings.bronze_settings import BronzeSettings
from core.settings.migrations_settings import MigrationsSettings
from tests.helpers.builders.migrations_silver_time_series_builder import MigrationsSilverTimeSeriesBuilder

scenarios("../features/stream_migration_to_bronze_migrated.feature")


# Given steps


@given("transactions available in the migration silver table")
def _(spark: SparkSession, create_external_resources, mock_checkpoint_path):
    silver_transactions = MigrationsSilverTimeSeriesBuilder(spark).add_row().build()

    table_helper.append_to_table(
        silver_transactions,
        MigrationsSettings().silver_database_name,
        MigrationsSilverTableNames.silver_time_series_table,
    )


# # When steps


@when("streaming daily load from Migration silver to Measurements Bronze")
def _(spark: SparkSession, mock_checkpoint_path):
    migrated_transactions_repository = MigratedTransactionsRepository(spark)
    migrations_silver_time_series_repository = MigrationsSilverTimeSeriesRepository(spark)

    latest_created_already_migrated = (
        migrated_transactions_repository.calculate_latest_created_timestamp_that_has_been_migrated()
    ) or datetime.datetime.min
    migrate_from_migrations.daily_load_of_migrations_to_measurements(
        migrations_silver_time_series_repository,
        migrated_transactions_repository,
        latest_created_already_migrated,
    )


@when("streaming full load from Migration silver to Measurements Bronze")
def _(spark: SparkSession, mock_checkpoint_path):
    migrated_transactions_repository = MigratedTransactionsRepository(spark)
    migrations_silver_time_series_repository = MigrationsSilverTimeSeriesRepository(spark)

    migrate_from_migrations.full_load_of_migrations_to_measurements(
        migrations_silver_time_series_repository,
        migrated_transactions_repository,
    )


# # Then steps


@then(
    parsers.parse(
        "between {min_transactions:d} and {max_transactions:d} transactions should be available in the bronze measurements migration table"
    )
)
def _(spark: SparkSession, min_transactions, max_transactions):
    row_count = spark.table(
        f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_migrated_transactions_table}"
    ).count()
    assert min_transactions <= row_count <= max_transactions, (
        f"Expected between {min_transactions} and {max_transactions} transactions, but found {row_count}"
    )
