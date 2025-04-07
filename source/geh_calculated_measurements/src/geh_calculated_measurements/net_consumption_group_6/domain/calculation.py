from datetime import datetime
from typing import Tuple

from geh_common.telemetry import use_span
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.net_consumption_group_6.domain import (
    Cenc,
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)
from geh_calculated_measurements.net_consumption_group_6.domain.cenc_daily import calculate_daily
from geh_calculated_measurements.net_consumption_group_6.domain.cenc_yearly import calculate_cenc


@use_span()
def execute(
    current_measurements: CurrentMeasurements,
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_zone: str,
    execution_start_datetime: datetime,
) -> Tuple[Cenc, DataFrame]:
    cenc = calculate_cenc(
        consumption_metering_point_periods,
        child_metering_points,
        current_measurements,
        time_zone,
        execution_start_datetime,
    )

    measurements = calculate_daily(
        current_measurements=current_measurements,
        cenc=cenc,
        time_zone=time_zone,
        execution_start_datetime=execution_start_datetime,
    )

    return cenc, measurements
