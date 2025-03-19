from unittest import mock

from pyspark.sql import SparkSession

import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config.table_names import TableNames as BronzeTableNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.silver_settings import SilverSettings
from core.silver.application.streams import migrated_transactions as mit
from core.silver.infrastructure.config import SilverTableNames
from tests.helpers.builders.migrated_transactions_builder import MigratedTransactionsBuilder


@mock.patch("core.silver.application.streams.migrated_transactions.spark_session.initialize_spark")
@mock.patch("core.silver.application.streams.migrated_transactions.MigratedTransactionsRepository")
@mock.patch("core.silver.application.streams.migrated_transactions.SilverMeasurementsRepository")
def test__migrated_transactions__should_call_expected(
    mock_SilverMeasurementsRepository,
    mock_MigratedTransactionsRepository,
    mock_initialize_spark,
) -> None:
    # Arrang
    mock_spark = mock.Mock()
    mock_initialize_spark.return_value = mock_spark

    mock_submitted_transactions = mock.Mock()
    mock_submitted_transactions.read_stream = mock.Mock()
    mock_MigratedTransactionsRepository.return_value = mock_submitted_transactions

    mock_write_measurements = mock.Mock()
    mock_write_measurements.write_stream = mock.Mock()
    mock_SilverMeasurementsRepository.return_value = mock_write_measurements

    # Act
    mit.stream_migrated_transactions_to_silver()

    # Assert
    mock_initialize_spark.assert_called_once()
    mock_MigratedTransactionsRepository.assert_called_once_with(mock_spark)
    mock_SilverMeasurementsRepository.assert_called_once()

    mock_submitted_transactions.read_stream.assert_called_once()
    mock_write_measurements.write_stream.assert_called_once()


@mock.patch("core.silver.application.streams.migrated_transactions.spark_session.initialize_spark")
def test__migrated_transactions__should_save_in_silver_measurements(
    mock_initialize_spark, mock_checkpoint_path, spark: SparkSession, migrations_executed
) -> None:
    # Arrange
    mock_initialize_spark.return_value = spark
    bronze_settings = BronzeSettings()
    silver_settings = SilverSettings()

    expected_transaction_id = "UnitTestingMigratedStream"
    bronze_migrated_transactions = MigratedTransactionsBuilder(spark).add_row(transaction_id=expected_transaction_id)

    table_helper.append_to_table(
        bronze_migrated_transactions.build(),
        bronze_settings.bronze_database_name,
        BronzeTableNames.bronze_migrated_transactions_table,
    )

    # Act
    mit.stream_migrated_transactions_to_silver()

    # Assert
    silver_table = spark.table(f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"transaction_id = '{expected_transaction_id}'"
    )
    assert silver_table.count() == 1


@mock.patch("core.silver.application.streams.migrated_transactions.SilverMeasurementsRepository.append_if_not_exists")
@mock.patch("core.silver.application.streams.migrated_transactions.migrations_transformation.transform")
@mock.patch("core.silver.application.streams.migrated_transactions.spark_session.initialize_spark")
def test__batch_operation__calls_expected_methods(
    mock_initialize_spark, mock_transform, mock_append_if_not_exists, spark
) -> None:
    # Arrange
    mock_initialize_spark.return_value = spark
    batch_id = 1

    mock_migrated_transactions = mock.Mock()
    mock_transformed_transactions = mock.Mock()
    mock_transform.return_value = mock_transformed_transactions

    # Act
    mit._batch_operation(mock_migrated_transactions, batch_id)

    # Assert
    mock_transform.assert_called_once_with(spark, mock_migrated_transactions)
    mock_append_if_not_exists.assert_called_once_with(silver_measurements=mock_transformed_transactions)
