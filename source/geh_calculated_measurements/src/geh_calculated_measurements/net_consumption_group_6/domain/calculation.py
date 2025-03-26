from datetime import datetime
from typing import Tuple

from geh_common.telemetry import use_span
from pyspark.sql import DataFrame

from geh_calculated_measurements.net_consumption_group_6.domain import (
    Cenc,
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
)
from geh_calculated_measurements.net_consumption_group_6.domain.cenc import calculate_cenc
from geh_calculated_measurements.net_consumption_group_6.domain.daily import calculate_daily


@use_span()
def execute(
    time_series_points: TimeSeriesPoints,
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_zone: str,
    execution_start_datetime: datetime,
) -> Tuple[Cenc, DataFrame]:
    cenc = calculate_cenc(
        consumption_metering_point_periods,
        child_metering_points,
        time_series_points,
        time_zone,
        execution_start_datetime,
    )

    measurements = calculate_daily(
        time_series_points=time_series_points,
        execution_start_datetime=execution_start_datetime,
        cenc=cenc,
        time_zone=time_zone,
    )

    return cenc, measurements
