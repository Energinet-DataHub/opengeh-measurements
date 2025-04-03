from unittest import mock

from pytest_mock import MockerFixture

from core.silver.application.streams import migrated_transactions as sut


def test__migrated_transactions__should_call_expected(
    mocker: MockerFixture,
) -> None:
    # Arrange
    mock_spark = mock.Mock()
    mock_initialize_spark = mocker.patch(f"{sut.__name__}.spark_session.initialize_spark", return_value=mock_spark)

    mock_migrated_transactions = mock.Mock()
    mock_migrated_transactions.read_stream = mock.Mock()
    mock_MigratedTransactionsRepository = mocker.patch(
        f"{sut.__name__}.MigratedTransactionsRepository", return_value=mock_migrated_transactions
    )

    mock_write_measurements = mock.Mock()
    mock_write_measurements.stream_migrated_transactions = mock.Mock()
    mock_SilverMeasurementsRepository = mocker.patch(
        f"{sut.__name__}.SilverMeasurementsStream", return_value=mock_write_measurements
    )

    # Act
    sut.stream_migrated_transactions_to_silver()

    # Assert
    mock_initialize_spark.assert_called_once()
    mock_MigratedTransactionsRepository.assert_called_once_with(mock_spark)
    mock_SilverMeasurementsRepository.assert_called_once()

    mock_migrated_transactions.read_stream.assert_called_once()
    mock_write_measurements.stream_migrated_transactions.assert_called_once()


def test__batch_operation__calls_expected_methods(spark, mocker: MockerFixture) -> None:
    # Arrange
    batch_id = 1
    mock_migrated_transactions = mock.Mock()
    mock_transformed_transactions = mock.Mock()
    mock_transform = mocker.patch(
        f"{sut.__name__}.migrations_transformation.transform", return_value=mock_transformed_transactions
    )
    mock_append_if_not_exists = mocker.patch(f"{sut.__name__}.SilverMeasurementsRepository.append_if_not_exists")
    mocker.patch(f"{sut.__name__}.spark_session.initialize_spark", return_value=spark)

    # Act
    sut._batch_operation(mock_migrated_transactions, batch_id)

    # Assert
    mock_transform.assert_called_once_with(mock_migrated_transactions)
    mock_append_if_not_exists.assert_called_once_with(silver_measurements=mock_transformed_transactions)
