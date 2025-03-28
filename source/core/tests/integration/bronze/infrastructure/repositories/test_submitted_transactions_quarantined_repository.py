from pyspark.sql import SparkSession

import tests.helpers.identifier_helper as identifier_helper
from core.bronze.infrastructure.config import BronzeTableNames
from core.bronze.infrastructure.repositories.submitted_transactions_quarantined_repository import (
    SubmittedTransactionsQuarantinedRepository,
)
from core.settings.bronze_settings import BronzeSettings
from tests.helpers.builders.submitted_transactions_quarantined_builder import SubmittedTransactionsQuarantinedBuilder


def test__append__should_append_to_table(spark: SparkSession, migrations_executed) -> None:
    # Arrange
    transaction_id = identifier_helper.generate_random_string()
    submitted_transactions_quarantined = (
        SubmittedTransactionsQuarantinedBuilder(spark).add_row(transaction_id=transaction_id).build()
    )

    # Act
    SubmittedTransactionsQuarantinedRepository().append(submitted_transactions_quarantined)

    # Assert
    actual = spark.read.table(
        f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_quarantined}"
    ).where(f"transaction_id = '{transaction_id}'")
    assert actual.count() == 1
    assert actual.schema == submitted_transactions_quarantined.schema  # TODO: fix this
    assert actual.collect() == submitted_transactions_quarantined.collect()
