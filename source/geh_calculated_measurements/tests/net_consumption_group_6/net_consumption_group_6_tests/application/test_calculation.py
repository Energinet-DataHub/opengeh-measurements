from datetime import datetime
from decimal import Decimal

from geh_common.domain.types import MeteringPointType, OrchestrationType
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.application.model import CalculatedMeasurementsInternal
from geh_calculated_measurements.net_consumption_group_6.application.calculation import (
    get_current_calculated_measurements,
)


def test_get_current_calculated_measurements(spark: SparkSession):
    # Arrange

    data = [
        # orchestration_type, orchestration_instance_id, transaction_id, transaction_creation_datetime, metering_point_id, metering_point_type, observation_time, quantity, settlement_type
        (
            OrchestrationType.NET_CONSUMPTION.value,
            "1",
            "1",
            datetime(2023, 1, 2, 0, 0),
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 3, 0, 0),
            "mp1",
            MeteringPointType.NET_CONSUMPTION.value,
            datetime(2023, 1, 1, 0, 0),
            Decimal(10.0),
            "up_to_end_of_period",
        ),  # Latest for mp1, obs1
        (
            OrchestrationType.NET_CONSUMPTION.value,
            "1",
            "1",
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 3, 0, 0),
            "mp1",
            MeteringPointType.NET_CONSUMPTION.value,
            datetime(2023, 1, 1, 0, 0),
            Decimal(5.0),
            "up_to_end_of_period",
        ),  # Older for mp1, obs1
        (
            OrchestrationType.NET_CONSUMPTION.value,
            "1",
            "1",
            datetime(2023, 1, 2, 0, 0),
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 3, 0, 0),
            "mp1",
            MeteringPointType.NET_CONSUMPTION.value,
            datetime(2023, 1, 2, 0, 0),
            Decimal(15.0),
            "up_to_end_of_period",
        ),  # Only record for mp1, obs2
        (
            OrchestrationType.NET_CONSUMPTION.value,
            "1",
            "1",
            datetime(2023, 1, 3, 0, 0),
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 3, 0, 0),
            "mp1",
            MeteringPointType.CONSUMPTION.value,
            datetime(2023, 1, 1, 0, 0),
            Decimal(20.0),
            "up_to_end_of_period",
        ),  # Different type
        (
            OrchestrationType.NET_CONSUMPTION.value,
            "1",
            "1",
            datetime(2023, 1, 2, 0, 0),
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 3, 0, 0),
            "mp2",
            MeteringPointType.NET_CONSUMPTION.value,
            datetime(2023, 1, 1, 0, 0),
            Decimal(25.0),
            "up_to_end_of_period",
        ),  # Latest for mp2, obs1
        (
            OrchestrationType.NET_CONSUMPTION.value,
            "1",
            "1",
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 3, 0, 0),
            "mp2",
            MeteringPointType.NET_CONSUMPTION.value,
            datetime(2023, 1, 1, 0, 0),
            Decimal(20.0),
            "up_to_end_of_period",
        ),  # Older for mp2, obs1
        (
            OrchestrationType.CAPACITY_SETTLEMENT.value,
            "1",
            "1",
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 1, 0, 0),
            datetime(2023, 1, 3, 0, 0),
            "mp2",
            MeteringPointType.CAPACITY_SETTLEMENT.value,
            datetime(2023, 1, 1, 0, 0),
            Decimal(20.0),
            "up_to_end_of_period",
        ),  # Wrong metering point type
    ]

    df = spark.createDataFrame(data, schema=CalculatedMeasurementsInternal.schema)

    calculated_measurements = CalculatedMeasurementsInternal(df)

    # Act
    result = get_current_calculated_measurements(calculated_measurements, MeteringPointType.NET_CONSUMPTION).df

    # Assert
    assert result.count() == 3  # 3 records after filtering and deduplication

    result_list = result.collect()

    # All records should have correct metering_point_type
    assert all(r.metering_point_type == MeteringPointType.NET_CONSUMPTION.value for r in result_list)

    # Check that we got the newest records for each combination
    mp1_obs1 = next(
        (r for r in result_list if r.metering_point_id == "mp1" and r.observation_time == datetime(2023, 1, 1, 0, 0)),
        None,
    )
    assert mp1_obs1 is not None
    assert mp1_obs1.quantity == 10.0

    mp1_obs2 = next(
        (r for r in result_list if r.metering_point_id == "mp1" and r.observation_time == datetime(2023, 1, 2, 0, 0)),
        None,
    )
    assert mp1_obs2 is not None
    assert mp1_obs2.quantity == 15.0

    mp2_obs1 = next(
        (r for r in result_list if r.metering_point_id == "mp2" and r.observation_time == datetime(2023, 1, 1, 0, 0)),
        None,
    )
    assert mp2_obs1 is not None
    assert mp2_obs1.quantity == 25.0


def test_get_current_calculated_measurements_empty(spark: SparkSession):
    # Arrange

    # Create an empty DataFrame with the required schema
    empty_df = spark.createDataFrame([], schema=CalculatedMeasurementsInternal.schema)

    calculated_measurements = CalculatedMeasurementsInternal(df=empty_df)

    # Act
    result = get_current_calculated_measurements(calculated_measurements, MeteringPointType.NET_CONSUMPTION).df

    # Assert
    assert result.count() == 0
