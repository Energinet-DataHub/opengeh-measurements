from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from geh_common.testing.dataframes import assert_schemas
from pytest_bdd import given, parsers, scenarios, then, when

import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.contracts.sap.sap_series_v1 import schema as sap_series_v1_schema
from core.gold.infrastructure.config import GoldTableNames, GoldViewNames
from core.settings.gold_settings import GoldSettings
from tests.helpers.builders.sap_series_builder import SAPSeriesBuilder

scenarios("../features/gold_sap_series_migrated_view.feature")


@given(
    "gold measurements with multiple transactions with one is of orcestration type migration",
    target_fixture="metering_point_id",
)
def _(spark):
    mp_id = identifier_helper.create_random_metering_point_id()

    data = (
        SAPSeriesBuilder(spark)
        .add_row(
            orchestration_type=GehCommonOrchestrationType.MIGRATION.value,
            metering_point_id=mp_id,
        )
        .add_row(
            orchestration_type=GehCommonOrchestrationType.SUBMITTED.value,
            metering_point_id=mp_id,
        )
        .build()
    )

    table_helper.append_to_table(data, GoldSettings().gold_database_name, GoldTableNames.gold_measurements_sap_series)
    return mp_id


@when("accessing the sap_series_migrated_v1 view", target_fixture="actual_schema")
def _(spark):
    return spark.table(f"{GoldSettings().gold_database_name}.{GoldViewNames.sap_series_migrated_v1}").schema


@when("querying the sap_series_migrated_v1 gold view for that metering point", target_fixture="actual_result")
def _(spark, metering_point_id):
    return spark.table(f"{GoldSettings().gold_database_name}.{GoldViewNames.sap_series_migrated_v1}").where(
        f"metering_point_id = '{metering_point_id}'"
    )


@then("the table schema should match the expected sap_series_v1 schema")
def _(actual_schema):
    assert_schemas.assert_schema(actual=actual_schema, expected=sap_series_v1_schema, ignore_nullability=True)


@then(parsers.parse("the result should contain {num_of_rows:d} rows"))
def _(actual_result, num_of_rows):
    assert actual_result.count() == num_of_rows
