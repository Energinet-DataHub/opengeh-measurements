from unittest import mock

from pyspark.sql import SparkSession

import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config.table_names import TableNames as BronzeTableNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.silver_settings import SilverSettings
from core.silver.application.streams import submitted_transactions as sut
from core.silver.infrastructure.config import SilverTableNames
from tests.helpers.builders.submitted_transactions_builder import SubmittedTransactionsBuilder, ValueBuilder


@mock.patch("core.silver.application.streams.submitted_transactions.spark_session.initialize_spark")
@mock.patch("core.silver.application.streams.submitted_transactions.BronzeRepository")
@mock.patch("core.silver.application.streams.submitted_transactions.SilverRepository")
def test__submitted_transactions__should_call_expected(
    mock_SilverRepository,
    mock_BronzeRepository,
    mock_initialize_spark,
) -> None:
    # Arrange
    mock_spark = mock.Mock()
    mock_initialize_spark.return_value = mock_spark
    mock_submitted_transactions = mock.Mock()
    mock_BronzeRepository.return_value = mock_submitted_transactions
    mock_write_measurements = mock.Mock()
    mock_SilverRepository.return_value = mock_write_measurements

    # Act
    sut.stream_submitted_transactions()

    # Assert
    mock_initialize_spark.assert_called_once()
    mock_BronzeRepository.assert_called_once()
    mock_SilverRepository.assert_called_once()


def test__submitted_transactions__should_save_in_silver_measurements(
    mock_checkpoint_path, spark: SparkSession, migrations_executed
) -> None:
    # Arrange
    bronze_settings = BronzeSettings()
    silver_settings = SilverSettings()
    expected_orchestration_id = identifier_helper.generate_random_string()
    value = ValueBuilder(spark).add_row(orchestration_instance_id=expected_orchestration_id).build()
    submitted_transactions = SubmittedTransactionsBuilder(spark).add_row(value=value).build()
    table_helper.append_to_table(
        submitted_transactions,
        bronze_settings.bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )

    # Act
    sut.stream_submitted_transactions()

    # Assert
    silver_table = spark.table(f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{expected_orchestration_id}'"
    )
    assert silver_table.count() == 1


def test__stream_submitted_transactions__when_invalid_should_save_in_bronze_submitted_transactions_quarantined(
    mock_checkpoint_path, spark: SparkSession, migrate
) -> None:
    # Arrange
    bronze_settings = BronzeSettings()
    expected_orchestration_id = identifier_helper.generate_random_string()
    value = (
        ValueBuilder(spark)
        .add_row(orchestration_instance_id=expected_orchestration_id, metering_point_type="MPT_UNSPECIFIED")
        .build()
    )
    submitted_transactions = SubmittedTransactionsBuilder(spark).add_row(value=value).build()
    table_helper.append_to_table(
        submitted_transactions,
        bronze_settings.bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )

    # Act
    sut.stream_submitted_transactions()

    # Assert
    bronze_submitted_transactions_quarantined_table = spark.table(
        f"{bronze_settings.bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_quarantined}"
    ).where(f"orchestration_instance_id = '{expected_orchestration_id}'")
    assert bronze_submitted_transactions_quarantined_table.count() == 1
