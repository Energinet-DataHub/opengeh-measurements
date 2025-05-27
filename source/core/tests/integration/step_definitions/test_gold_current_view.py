from datetime import datetime
from decimal import Decimal

from geh_common.data_products.measurements_core.measurements_gold.current_v1 import schema as current_v1_schema
from geh_common.testing.dataframes import assert_schemas
from pytest_bdd import given, parsers, scenarios, then, when

import tests.helpers.datetime_helper as datetime_helper
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.gold.infrastructure.config import GoldTableNames, GoldViewNames
from core.settings.gold_settings import GoldSettings
from tests.helpers.builders.gold_builder import GoldMeasurementsBuilder

scenarios("../features/gold_current_view.feature")


@given(
    "gold measurements with multiple transactions for the same metering point where only the latest is active",
    target_fixture="metering_point_id_and_expected_quantity",
)
def _(spark):
    mp_id = identifier_helper.create_random_metering_point_id()
    obs_time = datetime_helper.get_datetime()
    expected_quantity = Decimal(300)

    data = (
        GoldMeasurementsBuilder(spark)
        .add_row(
            metering_point_id=mp_id,
            observation_time=obs_time,
            quantity=Decimal(100),
            transaction_creation_datetime=datetime_helper.get_datetime(2021, 1, 1),
            is_cancelled=False,
        )
        .add_row(
            metering_point_id=mp_id,
            observation_time=obs_time,
            quantity=Decimal(200),
            transaction_creation_datetime=datetime_helper.get_datetime(2021, 1, 2),
            is_cancelled=True,
        )
        .add_row(
            metering_point_id=mp_id,
            observation_time=obs_time,
            quantity=expected_quantity,
            transaction_creation_datetime=datetime_helper.get_datetime(2021, 1, 3),
            is_cancelled=False,
        )
        .build()
    )

    table_helper.append_to_table(data, GoldSettings().gold_database_name, GoldTableNames.gold_measurements)
    return (mp_id, expected_quantity)


@given(
    "gold measurements with multiple transactions for the same metering point where the latest is cancelled",
    target_fixture="metering_point_id",
)
def _(spark):
    mp_id = identifier_helper.create_random_metering_point_id()
    obs_time = datetime_helper.get_datetime()

    data = (
        GoldMeasurementsBuilder(spark)
        .add_row(
            metering_point_id=mp_id,
            observation_time=obs_time,
            quantity=Decimal(100),
            transaction_creation_datetime=datetime_helper.get_datetime(2021, 1, 1),
            is_cancelled=False,
        )
        .add_row(
            metering_point_id=mp_id,
            observation_time=obs_time,
            quantity=Decimal(200),
            transaction_creation_datetime=datetime_helper.get_datetime(2021, 1, 2),
            is_cancelled=False,
        )
        .add_row(
            metering_point_id=mp_id,
            observation_time=obs_time,
            quantity=Decimal(300),
            transaction_creation_datetime=datetime_helper.get_datetime(2021, 1, 3),
            is_cancelled=True,
        )
        .build()
    )

    table_helper.append_to_table(data, GoldSettings().gold_database_name, GoldTableNames.gold_measurements)
    return mp_id


@given(parsers.parse("a gold measurement where {column} is null"), target_fixture="metering_point_id")
def _(spark, column):
    mp_id = identifier_helper.create_random_metering_point_id()

    kwargs = {
        "metering_point_id": mp_id,
        "observation_time": datetime.now(),
        "quality": "some_quality",
        "metering_point_type": "some_type",
    }
    kwargs[column] = None

    data = GoldMeasurementsBuilder(spark).add_row(**kwargs).build()
    table_helper.append_to_table(data, GoldSettings().gold_database_name, GoldTableNames.gold_measurements)
    return mp_id


@when("accessing the current_v1 gold view", target_fixture="actual_schema")
def _(spark):
    return spark.table(f"{GoldSettings().gold_database_name}.{GoldViewNames.current_v1}").schema


@when(
    "querying the current_v1 gold view for that metering point and quantity is null",
    target_fixture="actual_result_with_quantity_is_null",
)
def _(spark, metering_point_id_and_quantity_is_null):
    mp_id = metering_point_id_and_quantity_is_null
    return spark.table(f"{GoldSettings().gold_database_name}.{GoldViewNames.current_v1}").where(
        f"metering_point_id = '{mp_id}'"
    )


@when("querying the current_v1 gold view for that metering point", target_fixture="actual_result")
def _(spark, metering_point_id):
    return spark.table(f"{GoldSettings().gold_database_name}.{GoldViewNames.current_v1}").where(
        f"metering_point_id = '{metering_point_id}'"
    )


@when(
    "querying the current_v1 gold view for that metering point and expected quantity",
    target_fixture="actual_result_with_quantity",
)
def _(spark, metering_point_id_and_expected_quantity):
    mp_id, _ = metering_point_id_and_expected_quantity
    return spark.table(f"{GoldSettings().gold_database_name}.{GoldViewNames.current_v1}").where(
        f"metering_point_id = '{mp_id}'"
    )


@then("the table schema should match the expected current_v1 schema")
def _(actual_schema):
    assert_schemas.assert_schema(actual=actual_schema, expected=current_v1_schema, ignore_nullability=True)


@then(parsers.parse("the result should contain {num_of_rows:d} rows"))
def _(actual_result, num_of_rows):
    assert actual_result.count() == num_of_rows


@then("the result should contain 1 row with the expected quantity")
def _(actual_result_with_quantity, metering_point_id_and_expected_quantity):
    _, expected_quantity = metering_point_id_and_expected_quantity
    rows = actual_result_with_quantity.collect()
    assert len(rows) == 1
    assert rows[0]["quantity"] == expected_quantity
