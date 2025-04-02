from pytest_mock import MockerFixture

from core.silver.application.streams import submitted_transactions as sut


def test__submitted_transactions__should_call_expected(mocker: MockerFixture) -> None:
    # Arrange
    mock_initialize_spark = mocker.patch(f"{sut.__name__}.spark_session.initialize_spark")
    mock_SubmittedTransactionsRepository = mocker.patch(f"{sut.__name__}.SubmittedTransactionsRepository")
    mock_SilverMeasurementsRepository = mocker.patch(f"{sut.__name__}.SilverMeasurementsStream")

    # Act
    sut.stream_submitted_transactions()

    # Assert
    mock_initialize_spark.assert_called_once()
    mock_SubmittedTransactionsRepository.assert_called_once()
    mock_SilverMeasurementsRepository.assert_called_once()
