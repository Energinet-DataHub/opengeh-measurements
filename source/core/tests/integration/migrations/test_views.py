from datetime import datetime
from decimal import Decimal

import geh_common.testing.dataframes.assert_schemas as assert_schemas
import pytest
from geh_common.data_products.measurements_core.measurements_gold.current_v1 import current_v1
from pyspark.sql import SparkSession

import tests.helpers.datetime_helper as datetime_helper
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.gold.infrastructure.config import GoldTableNames, GoldViewNames
from core.settings.gold_settings import GoldSettings
from tests.helpers.builders.gold_builder import GoldMeasurementsBuilder


def test__current_view_v1__should_have_expected_schema(spark: SparkSession) -> None:
    # Arrange
    gold_settings = GoldSettings()

    # Assert
    actual_current = spark.table(f"{gold_settings.gold_database_name}.{GoldViewNames.current_v1}")
    assert_schemas.assert_schema(actual=actual_current.schema, expected=current_v1, ignore_nullability=True)


def test__current_view_v1__should_return_nothing_if_no_active_measurements_exists(spark: SparkSession) -> None:
    # Arrange
    gold_settings = GoldSettings()
    metering_point_id = identifier_helper.create_random_metering_point_id()
    observation_time = datetime_helper.get_datetime()

    gold_measurements = (
        GoldMeasurementsBuilder(spark)
        .add_row(
            metering_point_id=metering_point_id,
            transaction_creation_datetime=datetime_helper.get_datetime(year=2021, month=1, day=1),
            observation_time=observation_time,
            quantity=Decimal(100),
            is_cancelled=False,
        )
        .add_row(
            metering_point_id=metering_point_id,
            transaction_creation_datetime=datetime_helper.get_datetime(year=2021, month=1, day=2),
            observation_time=observation_time,
            quantity=Decimal(200),
            is_cancelled=False,
        )
        .add_row(
            metering_point_id=metering_point_id,
            transaction_creation_datetime=datetime_helper.get_datetime(year=2021, month=1, day=3),
            observation_time=observation_time,
            quantity=Decimal(300),
            is_cancelled=True,
        )
        .build()
    )

    table_helper.append_to_table(gold_measurements, gold_settings.gold_database_name, GoldTableNames.gold_measurements)

    # Act
    actual = spark.table(f"{gold_settings.gold_database_name}.{GoldViewNames.current_v1}").where(
        f"metering_point_id = '{metering_point_id}'"
    )

    # Assert
    assert actual.count() == 0


def test__current_v1__should_return_active_measurement_only(spark: SparkSession) -> None:
    # Arrange
    gold_settings = GoldSettings()
    metering_point_id = identifier_helper.create_random_metering_point_id()
    observation_time = datetime_helper.get_datetime()
    expected_quantity = Decimal(300)

    gold_measurements = (
        GoldMeasurementsBuilder(spark)
        .add_row(
            metering_point_id=metering_point_id,
            transaction_creation_datetime=datetime_helper.get_datetime(year=2021, month=1, day=1),
            observation_time=observation_time,
            quantity=Decimal(100),
            is_cancelled=False,
        )
        .add_row(
            metering_point_id=metering_point_id,
            transaction_creation_datetime=datetime_helper.get_datetime(year=2021, month=1, day=2),
            observation_time=observation_time,
            quantity=Decimal(200),
            is_cancelled=True,
        )
        .add_row(
            metering_point_id=metering_point_id,
            transaction_creation_datetime=datetime_helper.get_datetime(year=2021, month=1, day=3),
            observation_time=observation_time,
            quantity=expected_quantity,
            is_cancelled=False,
        )
        .build()
    )

    table_helper.append_to_table(gold_measurements, gold_settings.gold_database_name, GoldTableNames.gold_measurements)

    # Act
    actual = spark.table(f"{gold_settings.gold_database_name}.{GoldViewNames.current_v1}").where(
        f"metering_point_id = '{metering_point_id}'"
    )

    # Assert
    assert actual.count() == 1
    assert actual.collect()[0]["quantity"] == expected_quantity


@pytest.mark.parametrize(
    "metering_point_type, observation_time, quantity, quality",
    [
        (None, datetime.now(), Decimal(100), "some_quality"),
        ("some_type", None, Decimal(100), "some_quality"),
        ("some_type", datetime.now(), None, "some_quality"),
        ("some_type", datetime.now(), Decimal(100), None),
    ],
)
def test__current_view_v1__when_given_column_is_null__should_not_be_returned_by_view(
    metering_point_type: str,
    observation_time: datetime,
    quantity: Decimal,
    quality: str,
    spark: SparkSession,
) -> None:
    # Arrange
    gold_settings = GoldSettings()
    metering_point_id = identifier_helper.create_random_metering_point_id()

    gold_measurements = (
        GoldMeasurementsBuilder(spark)
        .add_row(
            metering_point_id=metering_point_id,
            metering_point_type=metering_point_type,
            observation_time=observation_time,
            quantity=quantity,
            quality=quality,
        )
        .build()
    )

    table_helper.append_to_table(gold_measurements, gold_settings.gold_database_name, GoldTableNames.gold_measurements)

    # Act
    actual = spark.table(f"{gold_settings.gold_database_name}.{GoldViewNames.current_v1}").where(
        f"metering_point_id = '{metering_point_id}'"
    )

    # Assert
    assert actual.count() == 0
