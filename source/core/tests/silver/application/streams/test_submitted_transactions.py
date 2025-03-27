from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config.table_names import TableNames as BronzeTableNames
from core.contracts.process_manager.enums.metering_point_type import MeteringPointType
from core.settings.bronze_settings import BronzeSettings
from core.settings.silver_settings import SilverSettings
from core.silver.application.streams import submitted_transactions as sut
from core.silver.infrastructure.config import SilverTableNames
from tests.helpers.builders.submitted_transactions_builder import SubmittedTransactionsBuilder, ValueBuilder


def test__submitted_transactions__should_call_expected(mocker: MockerFixture) -> None:
    # Arrange
    mock_initialize_spark = mocker.patch(f"{sut.__name__}.spark_session.initialize_spark")
    mock_BronzeRepository = mocker.patch(f"{sut.__name__}.BronzeRepository")
    mock_SilverMeasurementsRepository = mocker.patch(f"{sut.__name__}.SilverMeasurementsRepository")

    # Act
    sut.stream_submitted_transactions()

    # Assert
    mock_initialize_spark.assert_called_once()
    mock_BronzeRepository.assert_called_once()
    mock_SilverMeasurementsRepository.assert_called_once()


def test__submitted_transactions__should_save_in_silver_measurements(
    mock_checkpoint_path,
    spark: SparkSession,
    migrations_executed,
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
    mock_checkpoint_path, spark: SparkSession, migrations_executed
) -> None:
    # Arrange
    bronze_settings = BronzeSettings()
    expected_orchestration_id = identifier_helper.generate_random_string()
    value = (
        ValueBuilder(spark)
        .add_row(
            orchestration_instance_id=expected_orchestration_id,
            metering_point_type=MeteringPointType.MPT_UNSPECIFIED.value,
        )
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
