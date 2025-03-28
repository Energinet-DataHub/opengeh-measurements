import os
from unittest import mock

from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType

from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository


def test__write_stream__called__with_correct_arguments(mock_checkpoint_path: mock.MagicMock | mock.AsyncMock) -> None:
    # Arrange
    mocked_measurements = mock.Mock()
    mocked_batch_operation = mock.Mock()

    # Act
    SilverMeasurementsRepository().write_stream(
        mocked_measurements,
        GehCommonOrchestrationType.SUBMITTED,
        mocked_batch_operation,
    )

    # Assert
    mocked_measurements.writeStream.outputMode.assert_called_once_with("append")
    mocked_measurements.writeStream.outputMode().format().option().trigger.assert_called_once()
    mocked_measurements.writeStream.outputMode().format().option().trigger().foreachBatch.assert_called_once_with(
        mocked_batch_operation
    )
    mocked_measurements.writeStream.outputMode().format().option().trigger().foreachBatch().start.assert_called_once()
    mocked_measurements.writeStream.outputMode().format().option().trigger().foreachBatch().start().awaitTermination.assert_called_once()


def test__write_measurements__when_contionous_streaming_is_disabled__should_not_call_trigger() -> None:
    # Arrange
    os.environ["CONTINUOUS_STREAMING_ENABLED"] = "true"
    mocked_measurements = mock.Mock()
    mocked_batch_operation = mock.Mock()

    # Act
    SilverMeasurementsRepository().write_stream(
        mocked_measurements,
        GehCommonOrchestrationType.SUBMITTED,
        mocked_batch_operation,
    )

    # Assert
    mocked_measurements.writeStream.outputMode().option().trigger.assert_not_called()
