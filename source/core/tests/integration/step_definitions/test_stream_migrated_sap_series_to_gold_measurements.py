from pyspark.sql import SparkSession
from pytest_bdd import given, parsers, scenarios, then, when
from pytest_mock import MockerFixture

import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config.table_names import TableNames as BronzeTableNames
from core.gold.application.streams import migrated_sap_series_stream as sut
from core.gold.infrastructure.config import GoldTableNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.gold_settings import GoldSettings
from tests.helpers.builders.migrated_transactions_builder import MigratedTransactionsBuilder

scenarios("../features/stream_migrated_bronze_measurements_to_gold_measurements.feature")


# Given steps


@given(
    "valid migrated transactions inserted into the bronze migrated transactions table",
    target_fixture="expected_transaction_id",
)
def _(spark: SparkSession, mock_checkpoint_path, migrations_executed, mocker: MockerFixture):
    mocker.patch(f"{sut.__name__}.spark_session.initialize_spark", return_value=spark)

    transaction_id = identifier_helper.generate_random_string()
    bronze_migrated_transactions = MigratedTransactionsBuilder(spark).add_row(transaction_id=transaction_id)

    table_helper.append_to_table(
        bronze_migrated_transactions.build(),
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_migrated_transactions_table,
    )
    return transaction_id


# When steps


@when("streaming migrated transactions to the Gold layer")
def _(mock_checkpoint_path):
    sut.stream_migrated_transactions_to_sap_series_gold()


# Then steps


@then(parsers.parse("the transaction is available in the gold SAP Series table"))
def _(spark: SparkSession, expected_transaction_id):
    sap_series = spark.table(
        f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements_sap_series}"
    ).where(f"transaction_id = '{expected_transaction_id}'")
    assert sap_series.count() == 1, "Expected exactly one row in the gold SAP Series table for the transaction"
