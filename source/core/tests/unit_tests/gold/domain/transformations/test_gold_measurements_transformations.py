import datetime
import random
from decimal import Decimal

import geh_common.testing.dataframes.assert_schemas as assert_schemas
import pytest
from geh_common.domain.types.quantity_quality import QuantityQuality
from pyspark.sql import SparkSession

import core.gold.domain.transformations.gold_measurements_transformations as sut
from core.bronze.domain.constants.enums.quality import SubmittedTransactionsQuality
from core.gold.domain.constants.column_names.gold_measurements_column_names import GoldMeasurementsColumnNames
from core.gold.domain.schemas.gold_measurements import gold_measurements_schema
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder


def test__transform_silver_to_gold__should_match_gold_schema(spark: SparkSession) -> None:
    # Arrange
    df_silver = SilverMeasurementsBuilder(spark).add_row().build()

    # Act
    df_gold = sut.transform_silver_to_gold(df_silver)

    # Assert
    assert_schemas.assert_schema(actual=df_gold.schema, expected=gold_measurements_schema, ignore_nullability=True)


def test__explode_silver_points__should_explode_to_expected(spark: SparkSession) -> None:
    # Arrange
    df_silver = SilverMeasurementsBuilder(spark).add_row().build()

    # Act
    df_gold = sut.transform_silver_to_gold(df_silver)

    # Assert
    assert df_gold.count() == 24
    assert df_gold.select("metering_point_id").distinct().count() == 1
    assert df_gold.filter(df_gold["quality"].isNull()).count() == 0
    assert df_gold.filter(df_gold["quantity"].isNull()).count() == 0


def test__transform_silver_to_gold__monthly_resolution_first_day_of_month__should_return_correct_observation_time(
    spark: SparkSession,
) -> None:
    # Arrange
    start_date_time = datetime.datetime(2021, 1, 1, 0, 0, 0)
    df_silver = (
        SilverMeasurementsBuilder(spark)
        .add_row(
            resolution="PT1M",
            start_datetime=start_date_time,
            points=[
                {
                    "position": 1,
                    "quantity": Decimal(round(random.uniform(0, 1000), 3)),
                    "quality": "measured",
                }
            ],
        )
        .build()
    )

    # Act
    df_gold = sut.transform_silver_to_gold(df_silver)

    # Assert
    assert df_gold.count() == 1
    assert df_gold.collect()[0][GoldMeasurementsColumnNames.observation_time] == start_date_time


def test__transform_silver_to_gold__monthly_resolution_not_first_day_of_month__should_return_correct_observation_time(
    spark: SparkSession,
) -> None:
    # Arrange
    start_date_time = datetime.datetime(2021, 2, 2, 0, 0, 0)
    df_silver = (
        SilverMeasurementsBuilder(spark)
        .add_row(
            resolution="PT1M",
            start_datetime=start_date_time,
            points=[
                {
                    "position": 1,
                    "quantity": Decimal(round(random.uniform(0, 1000), 3)),
                    "quality": "measured",
                }
            ],
        )
        .build()
    )

    # Act
    df_gold = sut.transform_silver_to_gold(df_silver)

    # Assert
    assert df_gold.count() == 1
    assert df_gold.collect()[0][GoldMeasurementsColumnNames.observation_time] == start_date_time


def test__transform_silver_to_gold__hourly_resolution__returns_correct_observation_time(spark: SparkSession) -> None:
    # Arrange
    start_date_time = datetime.datetime(2021, 1, 1, 0, 0, 0)
    df_silver = (
        SilverMeasurementsBuilder(spark)
        .add_row(
            resolution="PT1H",
            start_datetime=start_date_time,
        )
        .build()
    )

    # Act
    df_gold = sut.transform_silver_to_gold(df_silver)

    # Assert
    assert df_gold.count() == 24
    for index, time in enumerate(df_gold.select(GoldMeasurementsColumnNames.observation_time).collect()):
        assert time[0] == start_date_time + datetime.timedelta(hours=index)


def test__transform_silver_to_gold__fifty_minutes_resolution__returns_correct_observation_time(
    spark: SparkSession,
) -> None:
    # Arrange
    start_date_time = datetime.datetime(2021, 1, 1, 0, 0, 0)
    df_silver = (
        SilverMeasurementsBuilder(spark)
        .add_row(
            resolution="PT15M",
            start_datetime=start_date_time,
        )
        .build()
    )

    # Act
    df_gold = sut.transform_silver_to_gold(df_silver)

    # Assert
    assert df_gold.count() == 24
    for index, time in enumerate(df_gold.select(GoldMeasurementsColumnNames.observation_time).collect()):
        assert time[0] == start_date_time + datetime.timedelta(minutes=index * 15)


@pytest.mark.parametrize(
    "submitted_quality, expected_quality",
    [
        pytest.param(SubmittedTransactionsQuality.Q_CALCULATED.value, QuantityQuality.CALCULATED.value),
        pytest.param(SubmittedTransactionsQuality.Q_ESTIMATED.value, QuantityQuality.ESTIMATED.value),
        pytest.param(SubmittedTransactionsQuality.Q_MEASURED.value, QuantityQuality.MEASURED.value),
        pytest.param(SubmittedTransactionsQuality.Q_MISSING.value, QuantityQuality.MISSING.value),
        pytest.param(
            SubmittedTransactionsQuality.Q_UNSPECIFIED.value, SubmittedTransactionsQuality.Q_UNSPECIFIED.value
        ),
    ],
)
def test__transform_silver_to_gold__when_quality_is_set__should_return_expected(
    submitted_quality, expected_quality, spark: SparkSession
) -> None:
    # Arrange
    builder = SilverMeasurementsBuilder(spark)
    point = builder.generate_point(quality=submitted_quality)

    silver_measurements = SilverMeasurementsBuilder(spark).add_row(points=[point]).build()

    # Act
    actual = sut.transform_silver_to_gold(silver_measurements)

    # Assert
    actual_row = actual.collect()[0]
    assert actual_row[GoldMeasurementsColumnNames.quality] == expected_quality
