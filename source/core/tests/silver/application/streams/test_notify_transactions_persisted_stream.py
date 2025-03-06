from unittest import mock

import core.silver.application.streams.notify_transactions_persisted_stream as sut


@mock.patch("core.silver.infrastructure.config.spark_session.initialize_spark")
@mock.patch(
    "core.silver.infrastructure.repositories.submitted_transactions_repository.SubmittedTransactionsRepository.read_submitted_transactions"
)
@mock.patch("core.silver.domain.transformations.transactions_persisted_events_transformation.transform")
@mock.patch("core.silver.infrastructure.streams.process_manager_stream.ProcessManagerStream.write_stream")
def test__notify__should_call_expected(
    mock_process_manager_stream_write_stream,
    mock_submitted_transactions_transformation,
    mocked_submitted_transactions_repository_read_submitted_transactions,
    mock_initialize_spark,
) -> None:
    # Arrange
    mock_spark = mock.Mock()
    mock_initialize_spark.return_value = mock_spark

    mock_submitted_transactions_repository = mock.Mock()
    mocked_submitted_transactions_repository_read_submitted_transactions.return_value = (
        mock_submitted_transactions_repository
    )

    mock_submitted_transactions = mock.Mock()
    mock_submitted_transactions_transformation.return_value = mock_submitted_transactions

    mock_write_stream = mock.Mock()
    mock_process_manager_stream_write_stream.return_value = mock_write_stream

    # Act
    sut.notify()

    # Assert
    mock_initialize_spark.assert_called_once()
    mocked_submitted_transactions_repository_read_submitted_transactions.assert_called_once()

    mock_submitted_transactions_transformation.assert_called_once_with(mock_submitted_transactions_repository)
    mock_process_manager_stream_write_stream.assert_called_once_with(mock_submitted_transactions)
