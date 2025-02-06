from pyspark.sql import SparkSession

from core.bronze.infrastructure.streams.bronze_repository import BronzeRepository


def test__read_measurements__should_read_from_bronze_measurements_table(
    spark: SparkSession, migrations_executed
) -> None:
    # Arrange
    bronze_repository = BronzeRepository(spark)

    # Act
    actual = bronze_repository.read_measurements()

    # Assert
    assert actual.isStreaming is True


def test__read_submitted_transactions__should_read_from_bronze_measurements_table(
    spark: SparkSession, migrations_executed
) -> None:
    # Arrange
    bronze_repository = BronzeRepository(spark)

    # Act
    actual = bronze_repository.read_submitted_transactions()

    # Assert
    assert actual.isStreaming is True
