from unittest.mock import Mock

from pytest_mock import MockerFixture

import core.bronze.application.submitted_transactions.unknown_submitted_transactions_handler as sut
from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    ValueColumnNames,
)


def test__handle__calls_expected(mocker: MockerFixture) -> None:
    mock_protobuf_versions = mocker.patch(f"{sut.__name__}.protobuf_versions.get_versions")
    mock_invalid_submitted_transactions_repository = mocker.patch.object(
        sut, sut.InvalidSubmittedTransactionsRepository.__name__
    )

    # Arrange
    submitted_transactions = Mock()

    # Act
    sut.handle(submitted_transactions)

    # Assert
    mock_protobuf_versions.assert_called_once()
    mock_invalid_submitted_transactions_repository().append.assert_called_once_with(
        submitted_transactions.filter.return_value
    )


def test__handle__filters_submitted_transactions_by_version(mocker: MockerFixture) -> None:
    # Arrange
    submitted_transactions = Mock()
    mock_protobuf_versions = mocker.patch(f"{sut.__name__}.protobuf_versions.get_versions")
    mock_protobuf_versions.return_value = ["1", "2", "3"]

    # Act
    sut.handle(submitted_transactions)

    # Assert
    submitted_transactions.filter.assert_called_once_with(f"'{ValueColumnNames.version}' not in (1,2,3)")
