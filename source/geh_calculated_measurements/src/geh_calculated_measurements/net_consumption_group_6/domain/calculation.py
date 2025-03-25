import uuid
from datetime import datetime
from typing import Tuple

from geh_common.telemetry import use_span

from geh_calculated_measurements.common.domain import CalculatedMeasurements
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
    orchestration_instance_id: uuid.UUID,
    execution_start_datetime: datetime,
) -> Tuple[Cenc, CalculatedMeasurements]:
    cenc = calculate_cenc(
        consumption_metering_point_periods,
        child_metering_points,
        time_series_points,
        time_zone,
        orchestration_instance_id,
        execution_start_datetime,
    )

    measurements = calculate_daily(cenc, consumption_metering_point_periods, child_metering_points, time_series_points)

    return cenc, measurements
