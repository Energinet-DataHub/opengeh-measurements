from pyspark.sql import SparkSession
from pytest_bdd import given, scenarios, then, when

import tests.helpers.identifier_helper as identifier_helper
from core.bronze.infrastructure.config import BronzeTableNames
from core.bronze.infrastructure.repositories.submitted_transactions_quarantined_repository import (
    SubmittedTransactionsQuarantinedRepository,
)
from core.settings.bronze_settings import BronzeSettings
from tests.helpers.builders.submitted_transactions_quarantined_builder import SubmittedTransactionsQuarantinedBuilder

scenarios("../features/append_quarantined_submitted_transactions.feature")


# Given steps


@given("quarantined submitted transactions", target_fixture="submitted_transactions_quarantined")
def _(spark: SparkSession):
    transaction_id = identifier_helper.generate_random_string()
    submitted_transactions_quarantined = (
        SubmittedTransactionsQuarantinedBuilder(spark).add_row(transaction_id=transaction_id).build()
    )
    # Attach the transaction_id as an attribute of the DataFrame to be used in the later steps
    submitted_transactions_quarantined.transaction_id = transaction_id  # type: ignore

    return submitted_transactions_quarantined


# When steps


@when("appending the quarantined submitted transactions to the bronze submitted transactions quarantined table")
def _(spark: SparkSession, submitted_transactions_quarantined):
    SubmittedTransactionsQuarantinedRepository().append(submitted_transactions_quarantined)


# Then steps
@then("the quarantined submitted transactions are available in the bronze submitted transactions quarantined table")
def _(spark: SparkSession, submitted_transactions_quarantined):
    actual = spark.read.table(
        f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_quarantined}"
    ).where(f"transaction_id = '{submitted_transactions_quarantined.transaction_id}'")
    assert actual.count() == 1
    assert actual.schema == submitted_transactions_quarantined.schema
    assert actual.collect() == submitted_transactions_quarantined.collect()
