from datetime import datetime
from decimal import Decimal

import geh_common.testing.dataframes.assert_schemas as assert_schemas
import pytest
from pyspark.sql import SparkSession

import tests.helpers.datetime_helper as datetime_helper
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.contracts.capacity_settlement.v1.capacity_settlement_v1 import capacity_settlement_v1
from core.contracts.current_v1 import current_v1
from core.contracts.electrical_heating.v1.electrical_heating_v1 import electrical_heating_v1
from core.gold.infrastructure.config import GoldTableNames, GoldViewNames
from core.settings.gold_settings import GoldSettings
from tests.helpers.builders.gold_builder import GoldMeasurementsBuilder


def test__electrical_heating_view_v1__should_have_expected_schema(
    spark: SparkSession, migrations_executed: None
) -> None:
    # Arrange
    gold_settings = GoldSettings()

    # Assert
    actual_electrical_heating = spark.table(f"{gold_settings.gold_database_name}.{GoldViewNames.electrical_heating_v1}")
    assert_schemas.assert_schema(actual=actual_electrical_heating.schema, expected=electrical_heating_v1)


def test__electrical_heating_view_v1__should_return_active_measurement_only(
    spark: SparkSession, migrations_executed: None
) -> None:
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
        )
        .add_row(
            metering_point_id=metering_point_id,
            transaction_creation_datetime=datetime_helper.get_datetime(year=2021, month=1, day=2),
            observation_time=observation_time,
            quantity=Decimal(200),
        )
        .add_row(
            metering_point_id=metering_point_id,
            transaction_creation_datetime=datetime_helper.get_datetime(year=2021, month=1, day=3),
            observation_time=observation_time,
            quantity=expected_quantity,
        )
        .build()
    )

    table_helper.append_to_table(gold_measurements, gold_settings.gold_database_name, GoldTableNames.gold_measurements)

    # Act
    actual = spark.table(f"{gold_settings.gold_database_name}.{GoldViewNames.electrical_heating_v1}").where(
        f"metering_point_id = {metering_point_id}"
    )

    # Assert
    assert actual.count() == 1
    assert actual.collect()[0]["quantity"] == expected_quantity


def test__electrical_heating_view_v1__when_metering_point_id_is_null__should_not_be_returned_by_view(
    spark: SparkSession, migrations_executed: None
) -> None:
    # Arrange
    gold_settings = GoldSettings()
    metering_point_type = identifier_helper.generate_random_string()

    gold_measurements = (
        GoldMeasurementsBuilder(spark)
        .add_row(
            metering_point_id=None,
            metering_point_type=metering_point_type,
        )
        .build()
    )

    table_helper.append_to_table(gold_measurements, gold_settings.gold_database_name, GoldTableNames.gold_measurements)

    # Act
    actual = spark.table(f"{gold_settings.gold_database_name}.{GoldViewNames.electrical_heating_v1}").where(
        f"metering_point_type = '{metering_point_type}'"
    )

    # Assert
    assert actual.count() == 0


@pytest.mark.parametrize(
    "metering_point_type, quantity, observation_time",
    [
        (None, Decimal(100), datetime.now()),
        ("some_type", None, datetime.now()),
        ("some_type", Decimal(100), None),
    ],
)
def test__electrical_heating_view_v1__when_given_column_is_null__should_not_be_returned_by_view(
    metering_point_type: str,
    quantity: Decimal,
    observation_time: datetime,
    spark: SparkSession,
    migrations_executed: None,
) -> None:
    # Arrange
    gold_settings = GoldSettings()
    metering_point_id = identifier_helper.create_random_metering_point_id()

    gold_measurements = (
        GoldMeasurementsBuilder(spark)
        .add_row(
            metering_point_id=metering_point_id,
            metering_point_type=metering_point_type,
            quantity=quantity,
            observation_time=observation_time,
        )
        .build()
    )

    table_helper.append_to_table(gold_measurements, gold_settings.gold_database_name, GoldTableNames.gold_measurements)

    # Act
    actual = spark.table(f"{gold_settings.gold_database_name}.{GoldViewNames.electrical_heating_v1}").where(
        f"metering_point_id = {metering_point_id}"
    )

    # Assert
    assert actual.count() == 0


### Capacity Settlement tests


def test__capacity_settlement_v1__should_have_expected_schema(spark: SparkSession, migrations_executed: None) -> None:
    # Arrange
    gold_settings = GoldSettings()

    # Assert
    actual_capacity_settlement_v1 = spark.table(
        f"{gold_settings.gold_database_name}.{GoldViewNames.capacity_settlement_v1}"
    )
    assert_schemas.assert_schema(actual=actual_capacity_settlement_v1.schema, expected=capacity_settlement_v1)


def test__capacity_settlement_v1__should_return_active_measurement_only(
    spark: SparkSession, migrations_executed: None
) -> None:
    # Arrange
    gold_settings = GoldSettings()
    metering_point_id = identifier_helper.create_random_metering_point_id()
    observation_time = datetime_helper.get_datetime()
    expected_quantity = Decimal(300)
    metering_point_type = "capacity_settlement"

    gold_measurements = (
        GoldMeasurementsBuilder(spark)
        .add_row(
            metering_point_id=metering_point_id,
            transaction_creation_datetime=datetime_helper.get_datetime(year=2021, month=1, day=1),
            observation_time=observation_time,
            quantity=Decimal(100),
            metering_point_type=metering_point_type,
        )
        .add_row(
            metering_point_id=metering_point_id,
            transaction_creation_datetime=datetime_helper.get_datetime(year=2021, month=1, day=2),
            observation_time=observation_time,
            quantity=Decimal(200),
            metering_point_type=metering_point_type,
        )
        .add_row(
            metering_point_id=metering_point_id,
            transaction_creation_datetime=datetime_helper.get_datetime(year=2021, month=1, day=3),
            observation_time=observation_time,
            quantity=expected_quantity,
            metering_point_type=metering_point_type,
        )
        .build()
    )

    table_helper.append_to_table(gold_measurements, gold_settings.gold_database_name, GoldTableNames.gold_measurements)

    # Act
    actual = spark.table(f"{gold_settings.gold_database_name}.{GoldViewNames.capacity_settlement_v1}").where(
        f"metering_point_id = {metering_point_id}"
    )

    # Assert
    assert actual.count() == 1
    assert actual.collect()[0]["quantity"] == expected_quantity


def test__capacity_settlement_v1__when_metering_point_type_is_not_valid_ones__should_not_be_returned_by_view(
    spark: SparkSession,
    migrations_executed: None,
) -> None:
    # Arrange
    gold_settings = GoldSettings()
    metering_point_id_1 = identifier_helper.create_random_metering_point_id()
    metering_point_id_2 = identifier_helper.create_random_metering_point_id()
    metering_point_id_3 = identifier_helper.create_random_metering_point_id()

    gold_measurements = (
        GoldMeasurementsBuilder(spark)
        .add_row(metering_point_id=metering_point_id_1, metering_point_type="UNKNOWN")
        .add_row(metering_point_id=metering_point_id_2, metering_point_type="capacity_settlement")
        .add_row(metering_point_id=metering_point_id_3, metering_point_type="consumption")
        .build()
    )

    table_helper.append_to_table(gold_measurements, gold_settings.gold_database_name, GoldTableNames.gold_measurements)

    # Act
    actual = spark.table(f"{gold_settings.gold_database_name}.{GoldViewNames.capacity_settlement_v1}").where(
        f"metering_point_id in ('{metering_point_id_1}','{metering_point_id_2}','{metering_point_id_3}')"
    )

    # Assert
    assert actual.count() == 2


### Current vuew tests


def test__current_view_v1__should_have_expected_schema(spark: SparkSession, migrations_executed: None) -> None:
    # Arrange
    gold_settings = GoldSettings()

    # Assert
    actual_current = spark.table(f"{gold_settings.gold_database_name}.{GoldViewNames.current_v1}")
    assert_schemas.assert_schema(actual=actual_current.schema, expected=current_v1)


def test__current_view_v1__should_return_active_non_cancelled_measurement_only(
    spark: SparkSession, migrations_executed: None
) -> None:
    # Arrange
    gold_settings = GoldSettings()
    metering_point_id = identifier_helper.create_random_metering_point_id()
    observation_time = datetime_helper.get_datetime()
    expected_quantity = Decimal(200)

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
            quantity=expected_quantity,
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
    migrations_executed: None,
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
