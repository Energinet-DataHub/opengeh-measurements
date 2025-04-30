from datetime import datetime

from pyspark.sql import SparkSession
from pytest_bdd import given, scenarios, then, when
from pytest_mock import MockerFixture

import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config.table_names import TableNames as BronzeTableNames
from core.gold.application.streams import migrated_transactions_stream as mit
from core.gold.infrastructure.config import GoldTableNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.gold_settings import GoldSettings
from tests.helpers.builders.gold_builder import GoldMeasurementsBuilder
from tests.helpers.builders.migrated_transactions_builder import MigratedTransactionsBuilder

scenarios("../features/stream_migrated_bronze_measurements_to_gold_measurements.feature")


# Given steps


@given(
    "valid migrated transactions inserted into the bronze migrated transactions table",
    target_fixture="expected_transaction_id",
)
def _(spark: SparkSession, mock_checkpoint_path, migrations_executed, mocker: MockerFixture):
    mocker.patch(f"{mit.__name__}.spark_session.initialize_spark", return_value=spark)

    transaction_id = identifier_helper.generate_random_string()
    bronze_migrated_transactions = MigratedTransactionsBuilder(spark).add_row(transaction_id=transaction_id)

    table_helper.append_to_table(
        bronze_migrated_transactions.build(),
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_migrated_transactions_table,
    )
    return transaction_id


@given(
    "migrated transactions dated before 2017 inserted into the bronze migrated transactions table",
    target_fixture="expected_transaction_id",
)
def _(spark: SparkSession, mock_checkpoint_path, migrations_executed, mocker: MockerFixture):
    mocker.patch(f"{mit.__name__}.spark_session.initialize_spark", return_value=spark)

    expected_transaction_id = identifier_helper.generate_random_string()
    bronze_migrated_transactions = (
        MigratedTransactionsBuilder(spark)
        .add_row(
            transaction_id=expected_transaction_id,
            valid_from_date=datetime(2016, 12, 30, 23, 0, 0),
            valid_to_date=datetime(2016, 12, 31, 23, 0, 0),
        )
        .build()
    )

    table_helper.append_to_table(
        bronze_migrated_transactions,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_migrated_transactions_table,
    )
    return expected_transaction_id


@given(
    "duplicated valid migrated transactions inserted into the bronze migrated transactions table",
    target_fixture="expected_transaction_id",
)
def _(spark: SparkSession, mock_checkpoint_path, migrations_executed, mocker: MockerFixture):
    mocker.patch(f"{mit.__name__}.spark_session.initialize_spark", return_value=spark)

    transaction_id = identifier_helper.generate_random_string()
    bronze_migrated_transactions = MigratedTransactionsBuilder(spark).add_row(transaction_id=transaction_id).build()

    table_helper.append_to_table(
        bronze_migrated_transactions,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_migrated_transactions_table,
    )
    table_helper.append_to_table(
        bronze_migrated_transactions,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_migrated_transactions_table,
    )
    return transaction_id


@given(
    "valid migrated transaction inserted into the bronze migratied transactions table and the same transaction inserted into the gold table",
    target_fixture="expected_transaction_id",
)
def _(spark: SparkSession, mock_checkpoint_path, migrations_executed, mocker: MockerFixture):
    mocker.patch(f"{mit.__name__}.spark_session.initialize_spark", return_value=spark)

    transaction_id = identifier_helper.generate_random_string()

    # Migratied transactions
    bronze_migrated_transactions = MigratedTransactionsBuilder(spark).add_row(transaction_id=transaction_id).build()
    table_helper.append_to_table(
        bronze_migrated_transactions,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_migrated_transactions_table,
    )

    # Gold measurements
    gold_measurements = GoldMeasurementsBuilder(spark).add_row(transaction_id=transaction_id).build()
    table_helper.append_to_table(
        gold_measurements,
        GoldSettings().gold_database_name,
        GoldTableNames.gold_measurements,
    )
    return transaction_id


# When steps


@when("streaming migrated transactions to the Gold layer")
def _(mock_checkpoint_path):
    mit.stream_migrated_transactions_to_gold()


# Then steps


@then("valid migrated transactions are available in the gold measurements table")
def _(spark: SparkSession, expected_transaction_id):
    gold_measurements = spark.table(f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements}").where(
        f"transaction_id = '{expected_transaction_id}'"
    )
    assert gold_measurements.count() == 24


@then("no measurements are available in the gold measurements table")
def _(spark: SparkSession, expected_transaction_id):
    gold_table = spark.table(f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements}").where(
        f"transaction_id = '{expected_transaction_id}'"
    )
    assert gold_table.count() == 0
