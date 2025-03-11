import os
from unittest import mock

from core.gold.infrastructure.config import GoldTableNames
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.settings.gold_settings import GoldSettings
from core.settings.storage_account_settings import StorageAccountSettings


def test__start_write_stream__calls_expected(mock_checkpoint_path: mock.MagicMock | mock.AsyncMock):
    # Arrange
    mocked_measurements = mock.Mock()
    mocked_batch_operation = mock.Mock()

    expected_data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT
    expected_gold_container_name = GoldSettings().gold_container_name

    # Act
    GoldMeasurementsRepository().write_stream(
        mocked_measurements,
        mocked_batch_operation,
    )

    # Assert
    mocked_measurements.writeStream.format.assert_called_once_with("delta")
    mocked_measurements.writeStream.format().queryName.assert_called_once_with("measurements_silver_to_gold")
    mocked_measurements.writeStream.format().queryName().option().trigger.assert_called_once_with(availableNow=True)
    mocked_measurements.writeStream.format().queryName().option().trigger().foreachBatch.assert_called_once_with(
        mocked_batch_operation
    )

    mocked_measurements.writeStream.format().queryName().option().trigger().foreachBatch().start.assert_called_once()
    mocked_measurements.writeStream.format().queryName().option().trigger().foreachBatch().start().awaitTermination.assert_called_once()

    mock_checkpoint_path.assert_called_once_with(
        expected_data_lake_settings, expected_gold_container_name, GoldTableNames.gold_measurements
    )


def test__start_write_stream__when_contionous_streaming_is_disabled__should_not_call_trigger() -> None:
    # Arrange
    os.environ["CONTINUOUS_STREAMING_ENABLED"] = "true"
    mocked_measurements = mock.Mock()
    mocked_batch_operation = mock.Mock()

    # Act
    GoldMeasurementsRepository().write_stream(mocked_measurements, mocked_batch_operation)

    # Assert
    mocked_measurements.writeStream.format().queryName().option().trigger.assert_not_called()
