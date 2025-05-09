from unittest import mock

from pytest_mock import MockerFixture

from core.gold.application.streams import migrated_transactions_stream as mit
from core.gold.domain.constants.streaming.query_names import QueryNames


def test__migrated_transactions__should_call_expected(
    mocker: MockerFixture,
) -> None:
    # Arrange
    mock_spark = mock.Mock()
    mock_initialize_spark = mocker.patch(f"{mit.__name__}.spark_session.initialize_spark", return_value=mock_spark)

    mock_migrated_transactions = mock.Mock()
    mock_migrated_transactions.read_stream = mock.Mock()
    mock_MigratedTransactionsRepository = mocker.patch(
        f"{mit.__name__}.MigratedTransactionsRepository", return_value=mock_migrated_transactions
    )

    mock_write_measurements = mock.Mock()
    mock_write_measurements.stream_migrated_transactions = mock.Mock()
    mock_GoldMeasurementsRepository = mocker.patch(
        f"{mit.__name__}.GoldMeasurementsStream", return_value=mock_write_measurements
    )

    # Act
    mit.stream_migrated_transactions_to_gold()

    # Assert
    mock_initialize_spark.assert_called_once()
    mock_MigratedTransactionsRepository.assert_called_once_with(mock_spark)
    mock_GoldMeasurementsRepository.assert_called_once()

    mock_migrated_transactions.read_stream.assert_called_once()
    mock_write_measurements.write_stream.assert_called_once()


def test__batch_operation__calls_expected_methods(spark, mocker: MockerFixture) -> None:
    # Arrange
    batch_id = 1
    mock_migrated_transactions = mock.Mock()
    mock_filtered = mock.Mock()
    mock_transformed_to_silver_transactions = mock.Mock()
    mock_transformed_to_gold_transactions = mock.Mock()
    silver_mock_filter = mocker.patch(
        f"{mit.__name__}.silver_migrations_filters.filter_away_rows_older_than_2017",
        return_value=mock_filtered,
    )
    silver_mock_transform = mocker.patch(
        f"{mit.__name__}.silver_migrations_transformations.transform",
        return_value=mock_transformed_to_silver_transactions,
    )
    gold_mock_transform = mocker.patch(
        f"{mit.__name__}.gold_migrations_transformations.transform_silver_to_gold",
        return_value=mock_transformed_to_gold_transactions,
    )
    mock_append_if_not_exists = mocker.patch(f"{mit.__name__}.GoldMeasurementsRepository.append_if_not_exists")
    mocker.patch(f"{mit.__name__}.spark_session.initialize_spark", return_value=spark)

    # Act
    mit._batch_operation(mock_migrated_transactions, batch_id)

    # Assert
    silver_mock_filter.assert_called_once_with(mock_migrated_transactions)
    silver_mock_transform.assert_called_once_with(mock_filtered)
    gold_mock_transform.assert_called_once_with(mock_transformed_to_silver_transactions)
    mock_append_if_not_exists.assert_called_once_with(
        mock_transformed_to_gold_transactions,
        query_name=QueryNames.MIGRATIONS_TO_GOLD,
    )
