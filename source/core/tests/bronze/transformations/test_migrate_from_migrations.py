import datetime
from unittest.mock import Mock

import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

import core.bronze.application.batch_scripts.migrate_from_migrations as migrate_from_migrations
from core.bronze.domain.schemas.migrated import migrated_schema
from core.bronze.infrastructure.migrated_transactions_repository import MigratedTransactionsRepository
from tests.bronze.helpers.builders.migrations_silver_time_series_builder import MigrationsSilverTimeSeriesBuilder


def test__migrate_from_migrations_to_measurements__given_no_already_loaded_data__then_perform_full_load(
    spark: SparkSession,
) -> None:
    # Arrange
    NUM_TRANSFERED = 1000
    migrations_time_series_data = MigrationsSilverTimeSeriesBuilder(spark)
    for i in range(NUM_TRANSFERED):
        migrations_time_series_data.add_row(metering_point_id=i)
    migrations_time_series_data = migrations_time_series_data.build()

    mock_migrations_repository = Mock()
    mock_migrations_repository.read_migrations_silver_time_series.return_value = migrations_time_series_data

    mock_measurements_repository = Mock()
    real_measurements_repository = MigratedTransactionsRepository(spark)
    mock_measurements_repository.calculate_latest_created_timestamp_that_has_been_migrated.return_value = None
    mock_measurements_repository.write_measurements_bronze_migrated = (
        real_measurements_repository.write_measurements_bronze_migrated
    )

    migrated_repository = MigratedTransactionsRepository(spark)
    count_before = migrated_repository.read_measurements_bronze_migrated().count()

    # Act
    migrate_from_migrations.migrate_from_migrations_to_measurements(
        migrated_transactions_repository=mock_measurements_repository,
        migrations_silver_time_series_repository=mock_migrations_repository,
    )

    # Assert
    result = migrated_repository.read_measurements_bronze_migrated()

    assert result.count() == count_before + NUM_TRANSFERED
    assert_schemas.assert_schema(result.schema, migrated_schema)


def test__migrate_from_migrations_to_measurements__given_some_already_loaded_data__then_perform_daily_load(
    spark: SparkSession,
) -> None:
    # Arrange
    NUM_TRANSFERED = 500
    OLD_DATA = 1000
    migrations_time_series_data = MigrationsSilverTimeSeriesBuilder(spark)
    for i in range(NUM_TRANSFERED):
        migrations_time_series_data.add_row(
            metering_point_id=i, created=datetime.datetime.now() - datetime.timedelta(days=1)
        )

    for i in range(OLD_DATA):
        migrations_time_series_data.add_row(
            metering_point_id=i, created=datetime.datetime.now() - datetime.timedelta(days=2)
        )
    migrations_time_series_data = migrations_time_series_data.build()

    mock_migrations_repository = Mock()
    mock_migrations_repository.read_migrations_silver_time_series.return_value = migrations_time_series_data

    mock_measurements_repository = Mock()
    real_measurements_repository = MigratedTransactionsRepository(spark)
    mock_measurements_repository.calculate_latest_created_timestamp_that_has_been_migrated.return_value = (
        datetime.datetime.now() - datetime.timedelta(days=2)
    )

    mock_measurements_repository.write_measurements_bronze_migrated = (
        real_measurements_repository.write_measurements_bronze_migrated
    )

    migrated_repository = MigratedTransactionsRepository(spark)
    count_before = migrated_repository.read_measurements_bronze_migrated().count()

    # Act
    migrate_from_migrations.migrate_from_migrations_to_measurements(
        migrated_transactions_repository=mock_measurements_repository,
        migrations_silver_time_series_repository=mock_migrations_repository,
    )

    # Assert
    result = migrated_repository.read_measurements_bronze_migrated()

    assert result.count() == count_before + NUM_TRANSFERED
    assert_schemas.assert_schema(result.schema, migrated_schema)
