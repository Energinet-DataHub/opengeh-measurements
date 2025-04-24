from geh_common.domain.types.metering_point_resolution import MeteringPointResolution
from pyspark.sql import SparkSession
from pytest_bdd import given, parsers, scenarios, then, when

import core.silver.entry_points as sut
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config.table_names import TableNames as BronzeTableNames
from core.contracts.process_manager.enums.resolution import Resolution
from core.settings.bronze_settings import BronzeSettings
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config.table_names import TableNames as SilverTableNames
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder
from tests.helpers.builders.submitted_transactions_builder import (
    PointsBuilder,
    SubmittedTransactionsBuilder,
    ValueBuilder,
)

scenarios("../features/stream_bronze_to_silver.feature")


# Given steps


@given(
    "valid submitted transactions inserted into the bronze submitted table",
    target_fixture="identifier",
)
def _(spark: SparkSession):
    orchestration_instance_id = identifier_helper.generate_random_string()
    value = ValueBuilder(spark).add_row(orchestration_instance_id=orchestration_instance_id).build()
    submitted_transaction = (
        SubmittedTransactionsBuilder(spark).add_row(value=value, topic=orchestration_instance_id).build()
    )
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return orchestration_instance_id


@given(
    "duplicated valid submitted transactions inserted into the bronze submitted table",
    target_fixture="identifier",
)
def _(spark: SparkSession):
    orchestration_instance_id = identifier_helper.generate_random_string()
    value = ValueBuilder(spark).add_row(orchestration_instance_id=orchestration_instance_id).build()
    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value).build()
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return orchestration_instance_id


@given("invalid submitted transactions inserted into the bronze submitted table", target_fixture="identifier")
def _(spark: SparkSession):
    topic = identifier_helper.generate_random_string()
    value = ValueBuilder(spark).add_row(version="-1").build()
    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value, topic=topic).build()

    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return topic


@given(
    "submitted transactions with unknown version inserted into the bronze submitted table", target_fixture="identifier"
)
def _(spark: SparkSession):
    topic = identifier_helper.generate_random_string()
    value = identifier_helper.generate_random_binary()

    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value, topic=topic).build()

    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return topic


@given(
    "valid submitted transactions inserted into the silver measurements table and the same submitted transactions inserted into the bronze submitted table",
    target_fixture="identifier",
)
def _(spark: SparkSession):
    orchestration_instance_id = identifier_helper.generate_random_string()
    # Insert into silver measurements table
    silver_measurements = (
        SilverMeasurementsBuilder(spark)
        .add_row(orchestration_instance_id=orchestration_instance_id, resolution=MeteringPointResolution.HOUR.value)
        .build()
    )
    table_helper.append_to_table(
        silver_measurements,
        SilverSettings().silver_database_name,
        SilverTableNames.silver_measurements,
    )

    # Insert into bronze submitted transactions table
    value = (
        ValueBuilder(spark)
        .add_row(orchestration_instance_id=orchestration_instance_id, resolution=Resolution.R_PT1H.value)
        .build()
    )
    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value).build()
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return orchestration_instance_id


@given(
    parsers.parse("submitted transaction where the {field} has value {value}"),
    target_fixture="identifier",
)
def _(spark: SparkSession, field: str, value: str):
    orchestration_instance_id = identifier_helper.generate_random_string()
    value = (
        ValueBuilder(spark).add_row(**{field: value}, orchestration_instance_id=orchestration_instance_id).build()  # type: ignore
    )

    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value).build()

    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )

    return orchestration_instance_id


@given(
    parsers.parse("submitted transaction points where the quality has value {quality}"),
    target_fixture="identifier",
)
def _(spark: SparkSession, quality: str):
    orchestration_instance_id = identifier_helper.generate_random_string()
    points = PointsBuilder(spark).add_row(quality=quality).build()
    value = ValueBuilder(spark).add_row(points=points, orchestration_instance_id=orchestration_instance_id).build()
    submitted_transaction = SubmittedTransactionsBuilder(spark).add_row(value=value).build()
    table_helper.append_to_table(
        submitted_transaction,
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )
    return orchestration_instance_id


# When steps


@when("streaming submitted transactions to the Silver layer")
def _(mock_checkpoint_path):
    sut.stream_submitted_transactions()


# Then steps


@then(
    "submitted transaction is persisted into the bronze quarantine table and are not available in the silver measurements table"
)
def _(spark: SparkSession, identifier: str):
    silver_table = spark.table(f"{SilverSettings().silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{identifier}'"
    )

    bronze_quarantine_table = spark.table(
        f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_quarantined}"
    ).where(f"orchestration_instance_id = '{identifier}'")

    assert silver_table.count() == 0
    assert bronze_quarantine_table.count() == 1


@then("submitted transaction is persisted into the invalid bronze submitted transaction table")
def _(spark: SparkSession, identifier: str):
    bronze_invalid_table = spark.table(
        f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_invalid_submitted_transactions}"
    ).where(f"topic = '{identifier}'")

    assert bronze_invalid_table.count() == 1


@then("measurements are available in the silver measurements table")
def _(spark: SparkSession, identifier: str):
    silver_table = spark.table(f"{SilverSettings().silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{identifier}'"
    )
    assert silver_table.count() == 1

    # Ensuring that the submitted transaction is not in the invalid table
    bronze_invalid_table = spark.table(
        f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_invalid_submitted_transactions}"
    ).where(f"topic = '{identifier}'")
    assert bronze_invalid_table.count() == 0

    # Ensuring that the submitted transaction is not in the quarantine table
    bronze_quarantine_table = spark.table(
        f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_quarantined}"
    ).where(f"orchestration_instance_id = '{identifier}'")
    assert bronze_quarantine_table.count() == 0
