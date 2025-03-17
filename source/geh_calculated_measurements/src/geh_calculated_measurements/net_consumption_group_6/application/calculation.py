from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.domain import (
    TimeSeriesPoints,
    child_metering_points_v1,
    consumption_metering_point_periods_v1,
    time_series_points_v1,
)
from geh_calculated_measurements.net_consumption_group_6.application.net_consumption_group_6_args import (
    NetConsumptionGroup6Args,
)
from geh_calculated_measurements.net_consumption_group_6.domain import execute
from geh_calculated_measurements.net_consumption_group_6.domain.daily import ConsumptionMeteringPointPeriods
from geh_calculated_measurements.net_consumption_group_6.domain.model import ChildMeteringPoints


@use_span()
def execute_application(spark: SparkSession, args: NetConsumptionGroup6Args) -> None:
    # TODO: Replace this dummy code with the real implementation
    df = spark.createDataFrame([], schema=consumption_metering_point_periods_v1)
    df1 = spark.createDataFrame([], schema=child_metering_points_v1)
    df2 = spark.createDataFrame([], schema=time_series_points_v1)
    consumption_metering_points_periods = ConsumptionMeteringPointPeriods(df)
    child_metering_points = ChildMeteringPoints(df1)
    time_series_points = TimeSeriesPoints(df2)

    execute(
        consumption_metering_points_periods,
        child_metering_points,
        time_series_points,
    )
