import datetime

from pyspark.sql import SparkSession
from pytest_bdd import given, parsers, scenarios, then, when

import core.bronze.application.batch_scripts.migrate_from_migrations as migrate_from_migrations
import tests.helpers.identifier_helper as identifier_helper
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


@given(
    parsers.parse(
        "a transaction created {older_days:d} days ago and another created {newer_days:d} days ago in the migration silver table"
    ),
    target_fixture="expected_newer_metering_point_id",
)
def _(spark: SparkSession, create_external_resources, mock_checkpoint_path, older_days, newer_days):
    older_id = identifier_helper.create_random_metering_point_id()
    newer_id = identifier_helper.create_random_metering_point_id()

    builder = MigrationsSilverTimeSeriesBuilder(spark)

    older_tx = builder.add_row(
        metering_point_id=older_id,
        created=datetime.datetime.now() - datetime.timedelta(days=older_days),
    ).build()
    table_helper.append_to_table(
        older_tx,
        MigrationsSettings().silver_database_name,
        MigrationsSilverTableNames.silver_time_series_table,
    )

    newer_tx = builder.add_row(
        metering_point_id=newer_id,
        created=datetime.datetime.now() - datetime.timedelta(days=newer_days),
    ).build()
    table_helper.append_to_table(
        newer_tx,
        MigrationsSettings().silver_database_name,
        MigrationsSilverTableNames.silver_time_series_table,
    )

    return newer_id


@given("transactions available in the migration silver table", target_fixture="expected_metering_point_id")
def _(spark: SparkSession, create_external_resources, mock_checkpoint_path):
    metering_point_id = identifier_helper.create_random_metering_point_id()
    silver_transactions = MigrationsSilverTimeSeriesBuilder(spark).add_row(metering_point_id=metering_point_id).build()

    table_helper.append_to_table(
        silver_transactions,
        MigrationsSettings().silver_database_name,
        MigrationsSilverTableNames.silver_time_series_table,
    )
    return metering_point_id


# When steps


@when(
    parsers.parse(
        "streaming daily load from Migration silver to Measurements Bronze using a cutoff {number_of_days:d} days ago"
    )
)
def _(spark: SparkSession, mock_checkpoint_path, number_of_days):
    migrated_repo = MigratedTransactionsRepository(spark)
    silver_repo = MigrationsSilverTimeSeriesRepository(spark)
    cutoff = datetime.datetime.now() - datetime.timedelta(days=number_of_days)
    migrate_from_migrations.daily_load_of_migrations_to_measurements(silver_repo, migrated_repo, cutoff)


@when("streaming full load from Migration silver to Measurements Bronze")
def _(spark: SparkSession, mock_checkpoint_path):
    migrated_transactions_repository = MigratedTransactionsRepository(spark)
    migrations_silver_time_series_repository = MigrationsSilverTimeSeriesRepository(spark)

    migrate_from_migrations.full_load_of_migrations_to_measurements(
        migrations_silver_time_series_repository,
        migrated_transactions_repository,
    )


# Then steps


@then("only the newer transaction should be available in the bronze measurements migration table")
def _(spark: SparkSession, expected_newer_metering_point_id):
    df = spark.table(f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_migrated_transactions_table}")
    count = df.filter(f"metering_point_id = '{expected_newer_metering_point_id}'").count()
    total = df.count()
    assert count == 1, f"Expected newer transaction '{expected_newer_metering_point_id}' to be migrated"
    assert total == 1, f"Expected only one transaction in the bronze table, but found {total}"


@then("transactions should be available in the bronze measurements migration table")
def _(spark: SparkSession, expected_metering_point_id):
    result = (
        spark.table(f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_migrated_transactions_table}")
        .filter(f"metering_point_id = '{expected_metering_point_id}'")
        .count()
    )
    assert result > 0, f"Expected transactions for metering point id '{expected_metering_point_id}', but found none"
