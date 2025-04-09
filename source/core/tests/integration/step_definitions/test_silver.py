"""Streaming from Bronze to Silver feature tests."""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pytest_bdd import given, parsers, scenarios, then, when

import core.silver.entry_points as sut
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config.table_names import TableNames as BronzeTableNames
from core.contracts.process_manager.enums.metering_point_type import MeteringPointType
from core.contracts.process_manager.enums.orchestration_type import OrchestrationType
from core.contracts.process_manager.enums.resolution import Resolution
from core.contracts.process_manager.enums.unit import Unit
from core.settings.bronze_settings import BronzeSettings
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config.table_names import TableNames as SilverTableNames
from tests.helpers.builders.submitted_transactions_builder import (
    PointsBuilder,
    SubmittedTransactionsBuilder,
    ValueBuilder,
)

scenarios("../features/silver.feature")


@given("a submitted transaction with unspecified resolution", target_fixture="expected_orchestration_id")
def _(spark):
    """a submitted transaction with unspecified resolution."""
    orchestration_instance_id = identifier_helper.generate_random_string()
    value = (
        ValueBuilder(spark)
        .add_row(orchestration_instance_id=orchestration_instance_id, resolution=Resolution.R_UNSPECIFIED.value)
        .build()
    )
    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value).build()
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return orchestration_instance_id


@given("a submitted transaction with unspecified metering point type", target_fixture="expected_orchestration_id")
def _(spark):
    """a submitted transaction with unspecified metering point type."""
    orchestration_instance_id = identifier_helper.generate_random_string()
    value = (
        ValueBuilder(spark)
        .add_row(
            orchestration_instance_id=orchestration_instance_id,
            metering_point_type=MeteringPointType.MPT_UNSPECIFIED.value,
        )
        .build()
    )
    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value).build()
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return orchestration_instance_id


@given("a submitted transaction with unspecified unit", target_fixture="expected_orchestration_id")
def _(spark):
    """a submitted transaction with unspecified unit."""
    orchestration_instance_id = identifier_helper.generate_random_string()
    value = (
        ValueBuilder(spark)
        .add_row(
            orchestration_instance_id=orchestration_instance_id,
            unit=Unit.U_UNSPECIFIED.value,
        )
        .build()
    )
    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value).build()
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return orchestration_instance_id


@given("a submitted transaction with unspecified orchestration type", target_fixture="expected_orchestration_id")
def _(spark):
    """a submitted transaction with unspecified orchestration type."""
    orchestration_instance_id = identifier_helper.generate_random_string()
    value = (
        ValueBuilder(spark)
        .add_row(
            orchestration_instance_id=orchestration_instance_id,
            orchestration_type=OrchestrationType.OT_UNSPECIFIED.value,
        )
        .build()
    )
    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value).build()
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return orchestration_instance_id


@given("invalid submitted measurements inserted into the bronze submitted table", target_fixture="key")
def _(spark: SparkSession):
    """invalid submitted measurements inserted into the bronze submitted table."""
    key = identifier_helper.generate_random_binary()
    value = identifier_helper.generate_random_binary()
    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value, key=key).build()
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return key


@given("submitted measurements with unknown version inserted into the bronze submitted table", target_fixture="key")
def _(spark: SparkSession):
    """submitted measurements with unknown version inserted into the bronze submitted table."""
    key = identifier_helper.generate_random_binary()
    value = ValueBuilder(spark).add_row(version="9999999").build()
    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value, key=key).build()
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return key


@given(
    parsers.parse("measurements where the {field} has value {value}"),
    target_fixture="expected_orchestration_id",
)
def _(spark: SparkSession, field: str, value: str):
    """Generic step for setting a field on measurements."""

    orchestration_instance_id = identifier_helper.generate_random_string()

    # Normalize value if surrounded by backticks, necessary for parsing
    if value.startswith("`") and value.endswith("`"):
        value = value[1:-1]

    # Fields that go in PointsBuilder
    if field == "quality":
        points = PointsBuilder(spark).add_row(**{field: value}).build()  # type: ignore
        value_obj = (
            ValueBuilder(spark).add_row(points=points, orchestration_instance_id=orchestration_instance_id).build()
        )

    # All other fields go directly to ValueBuilder
    else:
        value_obj = (
            ValueBuilder(spark).add_row(**{field: value}, orchestration_instance_id=orchestration_instance_id).build()  # type: ignore
        )

    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value_obj).build()

    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )

    return orchestration_instance_id


@given(
    "new valid submitted measurements inserted into the bronze submitted table",
    target_fixture="expected_orchestration_id",
)
def _(spark: SparkSession):
    """new valid submitted measurements inserted into the bronze submitted table."""
    orchestration_instance_id = identifier_helper.generate_random_string()
    value = ValueBuilder(spark).add_row(orchestration_instance_id=orchestration_instance_id).build()
    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value).build()
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return orchestration_instance_id


@when("streaming the submitted transaction to the Silver layer")
def _(mock_checkpoint_path):
    """streaming the submitted transaction to the Silver layer."""
    sut.stream_submitted_transactions()


@then(
    "the transaction are persisted into the bronze quarantine table and are not available in the silver measurements table"
)
def _(spark: SparkSession, expected_orchestration_id: str):
    """are not available in the silver measurements table."""
    silver_table = spark.table(f"{SilverSettings().silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{expected_orchestration_id}'"
    )

    bronze_quarantine_table = spark.table(
        f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_quarantined}"
    ).where(f"orchestration_instance_id = '{expected_orchestration_id}'")

    assert silver_table.count() == 0
    assert bronze_quarantine_table.count() == 1


@then("measurements are persisted into the invalid bronze submitted transaction table")
def _(spark: SparkSession, key: str):
    """are not quarantined in the bronze quarantine table."""
    bronze_invalid_table = spark.table(
        f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_invalid_submitted_transactions}"
    ).where(F.col("key") == key)

    assert bronze_invalid_table.count() == 1


@then("the measurements are available in the silver measurements table")
def _(spark: SparkSession, expected_orchestration_id: str):
    """the measurements are available in the silver measurements table."""
    silver_table = spark.table(f"{SilverSettings().silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{expected_orchestration_id}'"
    )

    assert silver_table.count() == 1
