from unittest.mock import Mock

from pyspark.sql import SparkSession

from core.bronze.infrastructure.config import BronzeTableNames
from core.bronze.infrastructure.streams.bronze_repository import BronzeRepository
from core.settings.bronze_settings import BronzeSettings


def test__read_submitted_transactions__should_read_from_submitted_transactions_table(
    spark: SparkSession, migrations_executed
) -> None:
    # Arrange
    bronze_repository = BronzeRepository(spark)

    # Act
    actual = bronze_repository.read_submitted_transactions()

    # Assert
    assert actual.isStreaming is True


def test__read_submitted_transactions__should_read_from_submitted_transactions_table_v2() -> None:
    # Arrange
    mock_spark = Mock()
    bronze_repository = BronzeRepository(mock_spark)
    bronze_database_name = BronzeSettings().bronze_database_name

    # Act
    bronze_repository.read_submitted_transactions()

    # Assert
    mock_spark.readStream.format.assert_called_once_with("delta")
    mock_spark.readStream.format().option.assert_called_once_with("ignoreDeletes", "true")
    mock_spark.readStream.format().option().table.assert_called_once_with(
        f"{bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_table}"
    )
