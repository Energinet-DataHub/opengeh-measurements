from unittest.mock import Mock

from pytest_mock import MockerFixture

import core.silver.application.measurements.measurements_handler as sut


def test__handle__calls_expected(mocker: MockerFixture) -> None:
    # Arrange
    mock_submitted_transactions_to_silver_validation = mocker.patch(
        f"{sut.__name__}.submitted_transactions_to_silver_validation",
    )
    mock_silver_measurements_repository = mocker.patch.object(sut, sut.SilverMeasurementsRepository.__name__)
    mock_persist_submitted_transactions_quarantined = mocker.patch.object(
        sut,
        sut._persist_submitted_transactions_quarantined.__name__,
    )
    submitted_transactions = Mock()
    protobuf_message = Mock()

    mock_submitted_transactions_to_silver_validation.validate.return_value = (Mock(), Mock())

    # Act
    sut.handle(submitted_transactions, protobuf_message)

    # Assert
    protobuf_message.transform.assert_called_once_with(submitted_transactions)
    mock_submitted_transactions_to_silver_validation.validate.assert_called_once_with(
        protobuf_message.transform.return_value
    )
    mock_silver_measurements_repository().append_if_not_exists.assert_called_once_with(
        mock_submitted_transactions_to_silver_validation.validate.return_value[0]
    )
    mock_persist_submitted_transactions_quarantined.assert_called_once_with(
        mock_submitted_transactions_to_silver_validation.validate.return_value[1]
    )


def test__persist_submitted_tranactions_quarantined__calls_expected(mocker: MockerFixture) -> None:
    mock_submitted_transactions_quarantined_transformations = mocker.patch(
        f"{sut.__name__}.submitted_transactions_quarantined_transformations"
    )
    mock_submitted_transactions_quarantined_repository = mocker.patch.object(
        sut, sut.SubmittedTransactionsQuarantinedRepository.__name__
    )

    # Arrange
    invalid_measurements = Mock()

    # Act
    sut._persist_submitted_transactions_quarantined(invalid_measurements)

    # Assert
    mock_submitted_transactions_quarantined_transformations.map_silver_measurements_to_submitted_transactions_quarantined.assert_called_once_with(
        invalid_measurements
    )
    mock_submitted_transactions_quarantined_repository().append.assert_called_once_with(
        mock_submitted_transactions_quarantined_transformations.map_silver_measurements_to_submitted_transactions_quarantined.return_value
    )
