from decimal import Decimal

from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from geh_common.testing.dataframes import assert_schemas
from pytest_bdd import given, parsers, scenarios, then, when

import core.contracts.sap.current_sap_v1 as current_sap_v1
import tests.helpers.datetime_helper as datetime_helper
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.gold.infrastructure.config import GoldTableNames, GoldViewNames
from core.settings.gold_settings import GoldSettings
from tests.helpers.builders.gold_builder import GoldMeasurementsBuilder

scenarios("../features/gold_current_sap_migrated_view.feature")


@given(
    "gold measurements with multiple transactions with one is of orcestration type migration",
    target_fixture="metering_point_id",
)
def _(spark):
    mp_id = identifier_helper.create_random_metering_point_id()
    obs_time = datetime_helper.get_datetime()

    data = (
        GoldMeasurementsBuilder(spark)
        .add_row(
            metering_point_id=mp_id,
            orchestration_type=GehCommonOrchestrationType.MIGRATION.value,
            observation_time=obs_time,
            quantity=Decimal(100),
            transaction_creation_datetime=datetime_helper.get_datetime(2021, 1, 1),
            is_cancelled=False,
        )
        .add_row(
            metering_point_id=mp_id,
            orchestration_type=GehCommonOrchestrationType.SUBMITTED.value,
            observation_time=obs_time,
            quantity=Decimal(200),
            transaction_creation_datetime=datetime_helper.get_datetime(2021, 1, 2),
            is_cancelled=False,
        )
        .build()
    )

    table_helper.append_to_table(data, GoldSettings().gold_database_name, GoldTableNames.gold_measurements)
    return mp_id


@when("accessing the current_sap_migrated_v1 gold view", target_fixture="actual_schema")
def _(spark):
    return spark.table(f"{GoldSettings().gold_database_name}.{GoldViewNames.current_sap_migrated_v1}").schema


@when("querying the current_sap_migrated_v1 gold view for that metering point", target_fixture="actual_result")
def _(spark, metering_point_id):
    return spark.table(f"{GoldSettings().gold_database_name}.{GoldViewNames.current_sap_v1}").where(
        f"metering_point_id = '{metering_point_id}'"
    )


@then("the table schema should match the expected current_sap_v1 schema")
def _(actual_schema):
    assert_schemas.assert_schema(actual=actual_schema, expected=current_sap_v1.schema, ignore_nullability=True)


@then(parsers.parse("the result should contain {num_of_rows:d} rows"))
def _(actual_result, num_of_rows):
    assert actual_result.count() == num_of_rows
