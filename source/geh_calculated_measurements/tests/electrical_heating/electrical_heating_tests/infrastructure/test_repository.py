from datetime import datetime
from decimal import Decimal

from geh_common.testing.dataframes.assert_schemas import assert_schema
from pyspark.sql import Row, SparkSession

from geh_calculated_measurements.electrical_heating.domain.model.test import MyTable
from geh_calculated_measurements.electrical_heating.domain.model.time_series_pointsv2 import (
    TimeSeriesPointsV2,
    time_series_points_v1_contract_schema,
)

DEFAULT_METERING_POINT_ID = "1234567890123"
DEFAULT_METERING_POINT_TYPE = "consumption"
DEFAULT_QUANTITY = Decimal("999.123")
DEFAULT_DATE = datetime(2024, 3, 2, 23, 0)


def test__read_time_series_points__returns_expected_domain_object(
    spark: SparkSession,
) -> None:
    # Arrange
    row = {
        "metering_point_id": DEFAULT_METERING_POINT_ID,
        "metering_point_type": DEFAULT_METERING_POINT_TYPE,
        "observation_time": DEFAULT_DATE,
        "quantity": DEFAULT_QUANTITY,
    }
    df = spark.createDataFrame([Row(**row)], schema=TimeSeriesPointsV2.schema)

    # Act
    time_series_points = TimeSeriesPointsV2(df)

    # Assert

    assert time_series_points._df.count() == df.count()

    assert TimeSeriesPointsV2.schema == time_series_points.schema
    assert TimeSeriesPointsV2.schema == df.schema

    assert TimeSeriesPointsV2.observation_time == "observation_time"
    assert TimeSeriesPointsV2.metering_point_id == "metering_point_id"

    assert TimeSeriesPointsV2.schema == time_series_points_v1_contract_schema

    assert_schema(TimeSeriesPointsV2.schema, time_series_points_v1_contract_schema)

    assert DEFAULT_METERING_POINT_ID == time_series_points._df.collect()[0].metering_point_id
    assert DEFAULT_QUANTITY == time_series_points._df.collect()[0].quantity
    assert DEFAULT_DATE == time_series_points._df.collect()[0].observation_time


def test():
    table = MyTable()
    df = table.read()
    df.show()

    print(f"Field id: {table.metering_point_id}")
    print(f"Field time: {table.observation_time}")
    print(f"Schema: {table.schema.json()}")
