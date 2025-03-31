import datetime

import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession
from pytest_mock import MockFixture

import core.bronze.application.batch_scripts.migrate_from_migrations as migrate_from_migrations
from core.bronze.domain.schemas.migrated_transactions import migrated_transactions_schema
from tests.helpers.builders.migrations_silver_time_series_builder import MigrationsSilverTimeSeriesBuilder


def test__migrate_time_series_from_migrations_to_measurements__given_no_already_loaded_data__then_perform_full_load(
    spark: SparkSession,
    mocker: MockFixture,
) -> None:
    # Arrange
    NUM_TRANSFERED = 1000
    migrations_time_series_data = MigrationsSilverTimeSeriesBuilder(spark)
    for i in range(NUM_TRANSFERED):
        migrations_time_series_data.add_row(metering_point_id=i)
    migrations_time_series_data = migrations_time_series_data.build()

    assert migrations_time_series_data.count() == NUM_TRANSFERED

    mocker.patch(
        f"{migrate_from_migrations.__name__}.MigrationsSilverTimeSeriesRepository.read_migrations_silver_time_series",
        return_value=migrations_time_series_data,
    )
    mocker.patch(f"{migrate_from_migrations.__name__}.spark_session", return_value=spark)
    mocker.patch(
        f"{migrate_from_migrations.__name__}.MigratedTransactionsRepository.calculate_latest_created_timestamp_that_has_been_migrated",
        return_value=None,
    )
    mock_write_measurements_bronze_migrated = mocker.patch(
        f"{migrate_from_migrations.__name__}.MigratedTransactionsRepository.write_measurements_bronze_migrated"
    )

    # Act
    migrate_from_migrations.migrate_time_series_from_migrations_to_measurements()

    # Assert
    calls = mock_write_measurements_bronze_migrated.call_args_list
    assert len(calls) > 0

    written = 0
    for call in calls:
        args, kwargs = call
        written += args[0].count()
        assert_schemas.assert_schema(args[0].schema, migrated_transactions_schema)

    assert written == NUM_TRANSFERED


def test__migrate_time_series_from_migrations_to_measurements__given_some_already_loaded_data__then_perform_daily_load(
    spark: SparkSession,
    mocker: MockFixture,
) -> None:
    # Arrange
    NEW_DATA = 500
    OLD_DATA = 1000
    migrations_time_series_data = MigrationsSilverTimeSeriesBuilder(spark)
    for i in range(NEW_DATA):
        migrations_time_series_data.add_row(
            metering_point_id=i, created=datetime.datetime.now() - datetime.timedelta(days=1)
        )

    # This unit test simulates having OLD_DATA old rows that were loaded two days ago.
    # Under a daily load scenario, only the newer NEW_DATA data should be written.
    for i in range(OLD_DATA):
        migrations_time_series_data.add_row(
            metering_point_id=i, created=datetime.datetime.now() - datetime.timedelta(days=2)
        )
    migrations_time_series_data = migrations_time_series_data.build()

    mocker.patch(
        f"{migrate_from_migrations.__name__}.MigratedTransactionsRepository.calculate_latest_created_timestamp_that_has_been_migrated",
        return_value=(datetime.datetime.now() - datetime.timedelta(days=2)),
    )

    mocker.patch(
        f"{migrate_from_migrations.__name__}.MigrationsSilverTimeSeriesRepository.read_migrations_silver_time_series",
        return_value=migrations_time_series_data,
    )
    mocker.patch(f"{migrate_from_migrations.__name__}.spark_session", return_value=spark)
    mock_write_measurements_bronze_migrated = mocker.patch(
        f"{migrate_from_migrations.__name__}.MigratedTransactionsRepository.write_measurements_bronze_migrated"
    )

    # Act
    migrate_from_migrations.migrate_time_series_from_migrations_to_measurements()

    # Assert
    calls = mock_write_measurements_bronze_migrated.call_args_list
    assert len(calls) > 0

    written = 0
    for call in calls:
        args, kwargs = call
        written += args[0].count()
        assert_schemas.assert_schema(args[0].schema, migrated_transactions_schema)

    assert written == NEW_DATA
