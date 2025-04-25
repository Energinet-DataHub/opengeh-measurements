from pyspark.sql import SparkSession
from pytest_bdd import given, scenarios, then, when

import core.gold.application.streams.calculated_measurements_stream as sut
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.gold.infrastructure.config import GoldTableNames
from core.gold.infrastructure.config.external_view_names import ExternalViewNames
from core.settings.calculated_settings import CalculatedSettings
from core.settings.gold_settings import GoldSettings
from tests.helpers.builders.calculated_builder import CalculatedMeasurementsBuilder

scenarios("../features/stream_calculated_measurements_to_gold_measurements.feature")


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


# When steps


@when("streaming calculated measurements to the Gold layer")
def _(mock_checkpoint_path):
    sut.stream_measurements_calculated_to_gold()


# Then steps


@then("measurements are available in the gold measurements table")
def _(spark: SparkSession, expected_metering_point_id):
    gold_measurements = spark.table(f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements}").where(
        f"metering_point_id = '{expected_metering_point_id}'"
    )
    assert gold_measurements.count() == 1
