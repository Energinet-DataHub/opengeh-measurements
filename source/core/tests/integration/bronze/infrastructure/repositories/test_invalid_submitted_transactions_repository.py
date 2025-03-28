from pyspark.sql import SparkSession

import tests.helpers.identifier_helper as identifier_helper
from core.bronze.infrastructure.config import BronzeTableNames
from core.bronze.infrastructure.repositories.invalid_submitted_transactions_repository import (
    InvalidSubmittedTransactionsRepository,
)
from core.settings.bronze_settings import BronzeSettings
from tests.helpers.builders.invalid_submitted_transactions_builder import InvalidSubmittedTransactionsBuilder


def test__append__should_append_to_table(spark: SparkSession, migrations_executed) -> None:
    # Arrange
    topic = identifier_helper.generate_random_string()
    database_name = BronzeSettings().bronze_database_name  # type: ignore
    invalid_submitted_transactions = InvalidSubmittedTransactionsBuilder(spark).add_row(topic=topic).build()

    # Act
    InvalidSubmittedTransactionsRepository().append(invalid_submitted_transactions)

    # Assert
    actual = spark.read.table(f"{database_name}.{BronzeTableNames.bronze_invalid_submitted_transactions}").where(
        f"topic = '{topic}'"
    )
    assert actual.count() == 1
    assert actual.schema == invalid_submitted_transactions.schema
    assert actual.collect() == invalid_submitted_transactions.collect()
