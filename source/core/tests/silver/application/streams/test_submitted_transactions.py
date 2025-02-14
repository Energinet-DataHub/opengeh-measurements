from unittest import mock

import core.silver.application.streams.submitted_transactions as sut


@mock.patch("core.silver.application.streams.submitted_transactions.spark_session.initialize_spark")
@mock.patch("core.silver.application.streams.submitted_transactions.BronzeRepository")
@mock.patch(
    "core.silver.application.streams.submitted_transactions.submitted_transactions_transformation.created_by_packed_submitted_transactions"
)
@mock.patch(
    "core.silver.application.streams.submitted_transactions.measurements_transformation.create_by_unpacked_submitted_transactions"
)
@mock.patch("core.silver.application.streams.submitted_transactions.SilverRepository")
def test__submitted_transactions__should_call_expected(
    mock_SilverRepository,
    mock_measurements_transformation_create_by_submitted_transactions,
    mock_created_by_packed_submitted_transactions,
    mock_BronzeRepository,
    mock_initialize_spark,
) -> None:
    # Arrange
    mock_spark = mock.Mock()
    mock_initialize_spark.return_value = mock_spark
    mock_submitted_transactions = mock.Mock()
    mock_BronzeRepository.return_value = mock_submitted_transactions
    mock_unpacked_submitted_transactions = mock.Mock()
    mock_created_by_packed_submitted_transactions.return_value = mock_unpacked_submitted_transactions
    mock_measurements = mock.Mock()
    mock_measurements_transformation_create_by_submitted_transactions.return_value = mock_measurements
    mock_write_measurements = mock.Mock()
    mock_SilverRepository.return_value = mock_write_measurements

    # Act
    sut.stream_submitted_transactions()

    # Assert
    mock_initialize_spark.assert_called_once()
    mock_BronzeRepository.assert_called_once()
    mock_created_by_packed_submitted_transactions.assert_called_once()
    mock_measurements_transformation_create_by_submitted_transactions.assert_called_once()
    mock_SilverRepository.assert_called_once()
