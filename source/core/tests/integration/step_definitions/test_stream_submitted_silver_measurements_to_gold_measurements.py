from pyspark.sql import SparkSession
from pytest_bdd import given, scenarios, then, when

import core.gold.application.streams.gold_measurements_stream as sut_gold
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.gold.infrastructure.config import GoldTableNames
from core.receipts.infrastructure.config.table_names import CoreInternalTableNames
from core.settings.core_internal_settings import CoreInternalSettings
from core.settings.gold_settings import GoldSettings
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config.table_names import TableNames as SilverTableNames
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder

scenarios("../features/stream_submitted_silver_measurements_to_gold_measurements.feature")


# Given steps


@given(
    "valid measurements inserted into the silver measurements table",
    target_fixture="expected_metering_point_id",
)
def _(spark: SparkSession, mock_checkpoint_path):
    metering_point_id = identifier_helper.create_random_metering_point_id()
    silver_measurements = SilverMeasurementsBuilder(spark).add_row(metering_point_id=metering_point_id).build()
    table_helper.append_to_table(
        silver_measurements, SilverSettings().silver_database_name, SilverTableNames.silver_measurements
    )
    return metering_point_id


@given(
    "valid measurements with an orchestration instance id inserted into the silver measurements table",
    target_fixture="expected_orchestration_instance_id",
)
def _(spark: SparkSession, mock_checkpoint_path):
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
    return orchestration_instance_id


# When steps


@when("streaming Silver measurements to the Gold layer")
def _(mock_checkpoint_path):
    sut_gold.stream_measurements_silver_to_gold()


# Then steps


@then("measurements are available in the gold measurements table")
def _(spark: SparkSession, expected_metering_point_id):
    gold_measurements = spark.table(f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements}").where(
        f"metering_point_id = '{expected_metering_point_id}'"
    )
    assert gold_measurements.count() == 24


@then("a receipt entry is available in the process manager receipts table")
def _(spark: SparkSession, expected_orchestration_instance_id):
    receipts = spark.table(
        f"{CoreInternalSettings().core_internal_database_name}.{CoreInternalTableNames.process_manager_receipts}"
    ).where(f"orchestration_instance_id = '{expected_orchestration_instance_id}'")
    assert receipts.count() == 1
