import os
from unittest import mock

from core.settings.silver_settings import SilverSettings
from core.settings.storage_account_settings import StorageAccountSettings
from core.silver.infrastructure.streams.silver_repository import SilverRepository


def test__write_stream__called__with_correct_arguments(mock_checkpoint_path: mock.MagicMock | mock.AsyncMock) -> None:
    # Arrange
    mocked_measurements = mock.Mock()
    mocked_batch_operation = mock.Mock()
    expected_checkpoint_path = mock_checkpoint_path.return_value

    expected_data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT  # type: ignore
    expected_silver_container_name = SilverSettings().silver_container_name  # type: ignore

    # Act
    SilverRepository().write_stream(mocked_measurements, mocked_batch_operation)

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
    SilverRepository().write_stream(mocked_measurements, mocked_batch_operation)

    # Assert
    mocked_measurements.writeStream.outputMode().option().trigger.assert_not_called()
