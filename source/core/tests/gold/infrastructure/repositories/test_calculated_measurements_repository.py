from unittest import mock

from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository


def test__start_write_stream__calls_expected(mock_checkpoint_path: mock.MagicMock | mock.AsyncMock):
    # Arrange
    mocked_measurements = mock.Mock()
    mocked_batch_operation = mock.Mock()
    with mock.patch("core.gold.infrastructure.config.spark.initialize_spark"):
        # Act
        GoldMeasurementsRepository().write_stream(
            "measurements_calculated_to_gold",
            mocked_measurements,
            mocked_batch_operation,
        )

        # Assert
        mocked_measurements.writeStream.format.assert_called_once_with("delta")
        mocked_measurements.writeStream.format().queryName.assert_called_once_with("measurements_calculated_to_gold")
        mocked_measurements.writeStream.format().queryName().option().trigger.assert_called_once_with(availableNow=True)
        mocked_measurements.writeStream.format().queryName().option().trigger().foreachBatch.assert_called_once_with(
            mocked_batch_operation
        )

        mocked_measurements.writeStream.format().queryName().option().trigger().foreachBatch().start.assert_called_once()
        mocked_measurements.writeStream.format().queryName().option().trigger().foreachBatch().start().awaitTermination.assert_called_once()
