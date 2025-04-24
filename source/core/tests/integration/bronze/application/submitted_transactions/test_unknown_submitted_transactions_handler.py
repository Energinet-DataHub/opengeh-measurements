from pyspark.sql import SparkSession

import core.bronze.application.submitted_transactions.unknown_submitted_transactions_handler as sut
from core.bronze.infrastructure.config import BronzeTableNames
from core.settings.bronze_settings import BronzeSettings
from tests.helpers.builders.invalid_submitted_transactions_builder import InvalidSubmittedTransactionsBuilder


def test__handle__appends_expected(spark: SparkSession) -> None:
    # Arrange
    invalid_submitted_transactions = (
        InvalidSubmittedTransactionsBuilder(spark)
        .add_row(version="1", topic="not_expected")
        .add_row(version="-1", topic="expected")
        .build()
    )

    # Act
    sut.handle(invalid_submitted_transactions)

    # Assert
    actual = spark.read.table(
        f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_invalid_submitted_transactions}"
    ).where("topic = 'expected'")
    assert actual.count() == 1

    actual_not_expected = spark.read.table(
        f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_invalid_submitted_transactions}"
    ).where("topic = 'not_expected'")
    assert actual_not_expected.count() == 0
