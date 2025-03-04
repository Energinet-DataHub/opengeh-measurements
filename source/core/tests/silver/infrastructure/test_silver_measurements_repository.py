import os
from unittest import mock

from pyspark.sql import SparkSession

import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.settings.silver_settings import SilverSettings
from core.settings.storage_account_settings import StorageAccountSettings
from core.silver.infrastructure.config import SilverTableNames
from core.silver.infrastructure.streams.silver_measurements_repository import SilverMeasurementsRepository
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder


def test__write_stream__called__with_correct_arguments(mock_checkpoint_path: mock.MagicMock | mock.AsyncMock) -> None:
    # Arrange
    mocked_measurements = mock.Mock()
    mocked_batch_operation = mock.Mock()
    expected_checkpoint_path = mock_checkpoint_path.return_value

    expected_data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT
    expected_silver_container_name = SilverSettings().silver_container_name

    # Act
    SilverMeasurementsRepository().write_stream(mocked_measurements, mocked_batch_operation)

    # Assert
    mocked_measurements.writeStream.outputMode.assert_called_once_with("append")
    mocked_measurements.writeStream.outputMode().format().option.assert_called_once_with(
        "checkpointLocation", expected_checkpoint_path
    )
    mocked_measurements.writeStream.outputMode().format().option().trigger.assert_called_once()
    mocked_measurements.writeStream.outputMode().format().option().trigger().foreachBatch.assert_called_once_with(
        mocked_batch_operation
    )
    mocked_measurements.writeStream.outputMode().format().option().trigger().foreachBatch().start.assert_called_once()
    mocked_measurements.writeStream.outputMode().format().option().trigger().foreachBatch().start().awaitTermination.assert_called_once()

    mock_checkpoint_path.assert_called_once_with(
        expected_data_lake_settings, expected_silver_container_name, "submitted_transactions"
    )


def test__write_measurements__when_contionous_streaming_is_disabled__should_not_call_trigger() -> None:
    # Arrange
    os.environ["CONTINUOUS_STREAMING_ENABLED"] = "true"
    mocked_measurements = mock.Mock()
    mocked_batch_operation = mock.Mock()

    # Act
    SilverMeasurementsRepository().write_stream(mocked_measurements, mocked_batch_operation)

    # Assert
    mocked_measurements.writeStream.outputMode().option().trigger.assert_not_called()


def test__append_if_not_exists__when_row_already_exists_in_table__should_not_append(
    spark: SparkSession, migrate
) -> None:
    # Arrange
    silver_settings = SilverSettings()
    orchestration_instance_id = identifier_helper.generate_random_string()
    silver_measurements = (
        SilverMeasurementsBuilder(spark).add_row(orchestration_instance_id=orchestration_instance_id).build()
    )
    table_helper.append_to_table(
        silver_measurements,
        silver_settings.silver_database_name,
        SilverTableNames.silver_measurements,
    )

    # Act
    SilverMeasurementsRepository().append_if_not_exists(silver_measurements)

    # Arrange
    actual = spark.table(f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{orchestration_instance_id}'"
    )
    assert actual.count() == 1


def test__append_if_not_exists__when_not_exists_in_table__should_append(spark: SparkSession, migrate) -> None:
    # Arrange
    silver_settings = SilverSettings()
    orchestration_instance_id = identifier_helper.generate_random_string()
    silver_measurements = (
        SilverMeasurementsBuilder(spark).add_row(orchestration_instance_id=orchestration_instance_id).build()
    )

    # Act
    SilverMeasurementsRepository().append_if_not_exists(silver_measurements)

    # Arrange
    actual = spark.table(f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{orchestration_instance_id}'"
    )
    assert actual.count() == 1
