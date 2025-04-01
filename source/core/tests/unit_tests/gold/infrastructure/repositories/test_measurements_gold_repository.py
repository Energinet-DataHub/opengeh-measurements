import os
from unittest import mock

from pytest_mock import MockFixture

from core.gold.domain.constants.streaming.checkpoint_names import CheckpointNames
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository


def test__start_write_stream__calls_expected(
    mock_checkpoint_path: mock.MagicMock | mock.AsyncMock, mocker: MockFixture
) -> None:
    # Arrange
    mocked_measurements = mock.Mock()
    mocked_batch_operation = mock.Mock()
    mocker.patch("core.gold.infrastructure.config.spark.initialize_spark")
    # Act
    GoldMeasurementsRepository().write_stream(
        CheckpointNames.SILVER_TO_GOLD.value,
        QueryNames.SILVER_TO_GOLD.value,
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


def test__start_write_stream__when_contionous_streaming_is_disabled__should_not_call_trigger(
    mocker: MockFixture,
) -> None:
    # Arrange
    os.environ["CONTINUOUS_STREAMING_ENABLED"] = "true"
    mocked_measurements = mock.Mock()
    mocked_batch_operation = mock.Mock()
    mocker.patch("core.gold.infrastructure.config.spark.initialize_spark")
    # Act
    GoldMeasurementsRepository().write_stream(
        CheckpointNames.SILVER_TO_GOLD.value,
        QueryNames.SILVER_TO_GOLD.value,
        mocked_measurements,
        mocked_batch_operation,
    )

    # Assert
    mocked_measurements.writeStream.format().queryName().option().trigger.assert_not_called()
