from geh_common.data_products.measurements_core.measurements_gold.sap_delta_v1 import schema as sap_delta_v1_schema
from geh_common.testing.dataframes import assert_schemas
from pytest_bdd import scenarios, then, when

from core.gold.infrastructure.config import GoldViewNames
from core.settings.gold_settings import GoldSettings

scenarios("../features/gold_sap_delta_view.feature")


@when("accessing the sap_delta_v1 gold view", target_fixture="actual_schema")
def _(spark):
    return spark.table(f"{GoldSettings().gold_database_name}.{GoldViewNames.sap_delta_v1}").schema


@then("the table schema should match the expected sap_delta_v1 schema")
def _(actual_schema):
    assert_schemas.assert_schema(actual=actual_schema, expected=sap_delta_v1_schema, ignore_nullability=True)
