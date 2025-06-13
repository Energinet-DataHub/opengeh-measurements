from decimal import Decimal

from geh_common.domain.types.metering_point_type import MeteringPointType
from geh_common.domain.types.orchestration_type import OrchestrationType
from geh_common.domain.types.quantity_quality import QuantityQuality
from pyspark.sql import SparkSession
from pytest_bdd import given, parsers, scenarios, then, when

import core.gold.application.streams.calculated_measurements_stream as sut
import tests.helpers.datetime_helper as datetime_helper
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.gold.infrastructure.config import GoldTableNames
from core.gold.infrastructure.config.external_view_names import ExternalViewNames
from core.settings.calculated_settings import CalculatedSettings
from core.settings.gold_settings import GoldSettings
from tests.helpers.builders.calculated_builder import CalculatedMeasurementsBuilder
from tests.helpers.builders.gold_builder import GoldMeasurementsBuilder

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


@given(
    "duplicated calculated measurements inserted into the calculated measurements table",
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
    table_helper.append_to_table(
        calculated_measurements,
        CalculatedSettings().calculated_database_name,
        ExternalViewNames.calculated_measurements_v1,
    )
    return metering_point_id


@given(
    "calculated measurements inserted into the calculated measurements table and the same calculated measurements inserted into the gold table",
    target_fixture="expected_metering_point_id",
)
def _(spark: SparkSession, create_external_resources, mock_checkpoint_path):
    metering_point_id = identifier_helper.create_random_metering_point_id()
    observation_time = datetime_helper.random_datetime()

    # Calculated measurements
    calculated_measurements = (
        CalculatedMeasurementsBuilder(spark)
        .add_row(
            metering_point_id=metering_point_id,
            orchestration_type=OrchestrationType.CAPACITY_SETTLEMENT.value,
            quantity=Decimal(1.0),
            quantity_quality=QuantityQuality.CALCULATED.value,
            metering_point_type=MeteringPointType.CAPACITY_SETTLEMENT.value,
            observation_time=observation_time,
            transaction_creation_datetime=observation_time,
        )
        .build()
    )
    table_helper.append_to_table(
        calculated_measurements,
        CalculatedSettings().calculated_database_name,
        ExternalViewNames.calculated_measurements_v1,
    )

    # Gold measurements
    gold_measurements = (
        GoldMeasurementsBuilder(spark)
        .add_row(
            metering_point_id=metering_point_id,
            orchestration_type=OrchestrationType.CAPACITY_SETTLEMENT.value,
            quantity=Decimal(1.0),
            quality=QuantityQuality.CALCULATED.value,
            metering_point_type=MeteringPointType.CAPACITY_SETTLEMENT.value,
            observation_time=observation_time,
            transaction_creation_datetime=observation_time,
        )
        .build()
    )
    table_helper.append_to_table(
        gold_measurements,
        GoldSettings().gold_database_name,
        GoldTableNames.gold_measurements,
    )
    return metering_point_id


# When steps


@when("streaming calculated measurements to the Gold layer")
def _(mock_checkpoint_path):
    sut.stream_measurements_calculated_to_gold()


# Then steps


@then(parsers.parse("{number_of_measurements_rows} measurements row(s) are available in the gold measurements table"))
def _(spark: SparkSession, expected_metering_point_id, number_of_measurements_rows):
    gold_measurements = spark.table(f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements}").where(
        f"metering_point_id = '{expected_metering_point_id}'"
    )
    assert gold_measurements.count() == int(number_of_measurements_rows)


@then(parsers.parse("{number_of_measurements_rows} measurements row(s) are available in the gold sap series table"))
def _(spark: SparkSession, expected_metering_point_id, number_of_measurements_rows):
    gold_measurements = spark.table(
        f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements_sap_series}"
    ).where(f"metering_point_id = '{expected_metering_point_id}'")
    assert gold_measurements.count() == int(number_of_measurements_rows)
