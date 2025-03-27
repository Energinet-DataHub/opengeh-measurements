from unittest.mock import Mock

from core.bronze.infrastructure.config import BronzeTableNames
from core.bronze.infrastructure.repositories.submitted_transactions_repository import SubmittedTransactionsRepository
from core.settings.bronze_settings import BronzeSettings


def test__read__should_read_from_submitted_transactions_table_v2() -> None:
    # Arrange
    mock_spark = Mock()
    sut = SubmittedTransactionsRepository(mock_spark)
    bronze_database_name = BronzeSettings().bronze_database_name

    # Act
    sut.read()

    # Assert
    mock_spark.readStream.format.assert_called_once_with("delta")
    mock_spark.readStream.format().option.assert_called_once_with("ignoreDeletes", "true")
    mock_spark.readStream.format().option().table.assert_called_once_with(
        f"{bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_table}"
    )
