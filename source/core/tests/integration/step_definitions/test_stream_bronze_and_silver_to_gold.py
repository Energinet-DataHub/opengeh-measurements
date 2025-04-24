from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pytest_bdd import given, scenarios, then, when
from pytest_mock import MockerFixture

import core.gold.application.streams.calculated_measurements_stream as sut_calc
import core.gold.application.streams.gold_measurements_stream as sut_gold
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config.table_names import TableNames as BronzeTableNames
from core.gold.application.streams import migrated_transactions_stream as mit
from core.gold.infrastructure.config import GoldTableNames
from core.gold.infrastructure.config.external_view_names import ExternalViewNames
from core.receipts.infrastructure.config.table_names import CoreInternalTableNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.calculated_settings import CalculatedSettings
from core.settings.core_internal_settings import CoreInternalSettings
from core.settings.gold_settings import GoldSettings
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config.table_names import TableNames as SilverTableNames
from tests.helpers.builders.calculated_builder import CalculatedMeasurementsBuilder
from tests.helpers.builders.migrated_transactions_builder import MigratedTransactionsBuilder
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder

scenarios("../features/stream_bronze_and_silver_to_gold.feature")


@pytest.fixture
def silver_measurements_data():
    return "default_metering_point_id", "default_orchestration_instance_id"


@pytest.fixture
def expected_metering_point_id(silver_measurements_data):
    return silver_measurements_data[0]


@pytest.fixture
def expected_orchestration_instance_id(silver_measurements_data):
    return silver_measurements_data[1]


# Given steps


@given(
    "calculated measurements inserted into the calculated measurements table",
    target_fixture="expected_metering_point_id",
)
def _(spark: SparkSession, create_external_resources, mock_checkpoint_path):
    metering_point_id = identifier_helper.create_random_metering_point_id()
    calculated_measurements = CalculatedMeasurementsBuilder(spark).add_row(metering_point_id=metering_point_id).build()
    table_helper.append_to_table(
        calculated_measurements,
        CalculatedSettings().calculated_database_name,
        ExternalViewNames.calculated_measurements_v1,
    )
    return metering_point_id


@given(
    "valid measurements inserted into the silver measurements table",
    target_fixture="silver_measurements_data",
)
def _(spark: SparkSession, mock_checkpoint_path) -> tuple[str, str]:
    metering_point_id = identifier_helper.create_random_metering_point_id()
    orchestration_instance_id = identifier_helper.generate_random_string()
    silver_measurements = (
        SilverMeasurementsBuilder(spark)
        .add_row(metering_point_id=metering_point_id, orchestration_instance_id=orchestration_instance_id)
        .build()
    )
    table_helper.append_to_table(
        silver_measurements, SilverSettings().silver_database_name, SilverTableNames.silver_measurements
    )
    return metering_point_id, orchestration_instance_id


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
    bronze_migrated_transactions = MigratedTransactionsBuilder(spark)
    bronze_migrated_transactions.add_row(
        transaction_id=expected_transaction_id,
        valid_from_date=datetime(2016, 12, 30, 23, 0, 0),
        valid_to_date=datetime(2016, 12, 31, 23, 0, 0),
    )

    table_helper.append_to_table(
        bronze_migrated_transactions.build(),
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_migrated_transactions_table,
    )
    return expected_transaction_id


# When steps


@when("streaming calculated measurements to the Gold layer")
def _(mock_checkpoint_path):
    sut_calc.stream_measurements_calculated_to_gold()


@when("streaming Silver measurements to the Gold layer")
def _(mock_checkpoint_path):
    sut_gold.stream_measurements_silver_to_gold()


@when("streaming migrated transactions to the Gold layer")
def _(mock_checkpoint_path):
    mit.stream_migrated_transactions_to_gold()


# Then steps


@then("measurements are available in the calculated measurements table")
def _(spark: SparkSession, expected_metering_point_id):
    gold_measurements = spark.table(
        f"{CalculatedSettings().calculated_database_name}.{ExternalViewNames.calculated_measurements_v1}"
    ).where(f"metering_point_id = '{expected_metering_point_id}'")
    assert gold_measurements.count() == 1


@then("a receipt entry is available in the process manager receipts table")
def _(spark: SparkSession, expected_orchestration_instance_id):
    receipts = spark.table(
        f"{CoreInternalSettings().core_internal_database_name}.{CoreInternalTableNames.process_manager_receipts}"
    ).where(f"orchestration_instance_id = '{expected_orchestration_instance_id}'")
    assert receipts.count() == 1


@then("measurements are available in the gold measurements table")
def _(spark: SparkSession, expected_metering_point_id):
    gold_measurements = spark.table(f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements}").where(
        f"metering_point_id = '{expected_metering_point_id}'"
    )
    assert gold_measurements.count() == 24


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
