from unittest import mock

from pytest_mock import MockerFixture

import core.silver.application.streams.notify_transactions_persisted_stream as sut


def test__notify__should_call_expected(
    mocker: MockerFixture,
) -> None:
    # Arrange
    mock_submitted_transactions_repository = mock.Mock()
    mocked_submitted_transactions_repository_read_submitted_transactions = mocker.patch(
        f"{sut.__name__}.SilverMeasurementsRepository.read_submitted",
        return_value=mock_submitted_transactions_repository,
    )

    mock_submitted_transactions = mock.Mock()
    mock_submitted_transactions_transformation = mocker.patch(
        f"{sut.__name__}.transactions_persisted_events_transformation.transform",
        return_value=mock_submitted_transactions,
    )

    mock_write_stream = mock.Mock()
    mock_process_manager_stream_write_stream = mocker.patch(
        f"{sut.__name__}.ProcessManagerStream.write_stream",
        return_value=mock_write_stream,
    )

    # Act
    sut.notify()

    # Assert
    mocked_submitted_transactions_repository_read_submitted_transactions.assert_called_once()

    mock_submitted_transactions_transformation.assert_called_once_with(mock_submitted_transactions_repository)
    mock_process_manager_stream_write_stream.assert_called_once_with(mock_submitted_transactions)
