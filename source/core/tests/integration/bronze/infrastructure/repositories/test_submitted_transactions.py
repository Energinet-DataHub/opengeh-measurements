from pyspark.sql import SparkSession

from core.bronze.infrastructure.repositories.submitted_transactions_repository import SubmittedTransactionsRepository


def test__read_submitted_transactions__should_read_from_submitted_transactions_table(
    spark: SparkSession, migrations_executed
) -> None:
    # Arrange
    sut = SubmittedTransactionsRepository(spark)

    # Act
    actual = sut.read()

    # Assert
    assert actual.isStreaming is True
