from unittest import mock

import core.silver.application.streams.notify_transactions_persisted_stream as sut


@mock.patch("core.bronze.application.config.spark_session.initialize_spark")
@mock.patch(
    "core.silver.domain.transformations.submitted_transactions_transformation.create_by_packed_submitted_transactions"
)
@mock.patch(
    "core.silver.domain.transformations.transactions_persisted_events_transformation.create_by_unpacked_submitted_transactions"
)
@mock.patch(
    "core.silver.infrastructure.streams.submitted_transactions_repository.SubmittedTransactionsRepository.read_submitted_transactions"
)
@mock.patch("core.silver.infrastructure.streams.kafka_stream.KafkaStream.write_stream")
def test__notify__should_call_expected(
    mock_kafka_stream_write_stream,
    mocked_submitted_transactions_repository_read_submitted_transactions,
    mock_transactions_persisted_events_transformation,
    mock_submitted_transactions_transformation,
    mock_initialize_spark,
) -> None:
    # Arrange
    mock_spark = mock.Mock()
    mock_initialize_spark.return_value = mock_spark
    mock_submitted_transactions = mock.Mock()
    mock_submitted_transactions_transformation.return_value = mock_submitted_transactions
    mock_transactions_persisted_events = mock.Mock()
    mock_transactions_persisted_events_transformation.return_value = mock_transactions_persisted_events
    mock_submitted_transactions_repository = mock.Mock()
    mocked_submitted_transactions_repository_read_submitted_transactions.return_value = (
        mock_submitted_transactions_repository
    )
    mock_write_stream = mock.Mock()
    mock_kafka_stream_write_stream.return_value = mock_write_stream

    # Act
    sut.notify()

    # Assert
    mock_initialize_spark.assert_called_once()
    mocked_submitted_transactions_repository_read_submitted_transactions.assert_called_once()
    mock_submitted_transactions_transformation.assert_called_once_with(mock_submitted_transactions_repository)
    mock_transactions_persisted_events_transformation.assert_called_once_with(mock_submitted_transactions)
    mock_kafka_stream_write_stream.assert_called_once_with(mock_transactions_persisted_events)
