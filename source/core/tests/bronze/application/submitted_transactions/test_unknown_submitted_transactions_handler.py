from unittest.mock import Mock, patch

import core.bronze.application.submitted_transactions.unknown_submitted_transactions_handler as sut
from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    ValueColumnNames,
)


def test__handle__calls_expected() -> None:
    with (
        patch(
            "core.bronze.application.submitted_transactions.unknown_submitted_transactions_handler.ProtobufVersions"
        ) as mock_protobuf_versions,
        patch(
            "core.bronze.application.submitted_transactions.unknown_submitted_transactions_handler.InvalidSubmittedTransactionsRepository"
        ) as mock_invalid_submitted_transactions_repository,
    ):
        # Arrange
        submitted_transactions = Mock()

        # Act
        sut.handle(submitted_transactions)

        # Assert
        mock_protobuf_versions().get_versions.assert_called_once()
        mock_invalid_submitted_transactions_repository().append.assert_called_once_with(
            submitted_transactions.filter.return_value
        )


def test__handle__filters_submitted_transactions_by_version() -> None:
    with patch(
        "core.bronze.application.submitted_transactions.unknown_submitted_transactions_handler.ProtobufVersions"
    ) as mock_protobuf_versions:
        # Arrange
        submitted_transactions = Mock()
        mock_protobuf_versions().get_versions.return_value = ["1", "2", "3"]

        # Act
        sut.handle(submitted_transactions)

        # Assert
        submitted_transactions.filter.assert_called_once_with(f"{ValueColumnNames.version} not in (1,2,3)")
