from datetime import datetime
from typing import Tuple

from geh_common.telemetry import use_span

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.domain.model import CalculatedMeasurementsDaily
from geh_calculated_measurements.net_consumption_group_6.domain import (
    Cenc,
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)
from geh_calculated_measurements.net_consumption_group_6.domain.cnc_logic import execute_logic


@use_span()
def execute(
    current_measurements: CurrentMeasurements,
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_zone: str,
    execution_start_datetime: datetime,
) -> CalculatedMeasurementsDaily:
    measurements = execute_logic(
        current_measurements=current_measurements,
        consumption_metering_point_periods=consumption_metering_point_periods,
        child_metering_points=child_metering_points,
        time_zone=time_zone,
        execution_start_datetime=execution_start_datetime,
    )

    return CalculatedMeasurementsDaily(measurements)
