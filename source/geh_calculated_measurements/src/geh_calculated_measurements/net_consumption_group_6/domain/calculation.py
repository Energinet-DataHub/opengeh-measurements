from typing import Tuple

from geh_common.telemetry import use_span
from pyspark.sql import DataFrame

from geh_calculated_measurements.capacity_settlement.domain import TimeSeriesPoints
from geh_calculated_measurements.common.domain import CalculatedMeasurements
from geh_calculated_measurements.electrical_heating.domain import ChildMeteringPoints, ConsumptionMeteringPointPeriods
from geh_calculated_measurements.net_consumption_group_6.domain.cenc import Cenc, calculate_cenc
from geh_calculated_measurements.net_consumption_group_6.domain.daily import calculate_daily


@use_span()
def execute(
    time_series_points: TimeSeriesPoints,
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    internal_daily: DataFrame,
) -> Tuple[Cenc, CalculatedMeasurements]:
    orchestration_instance_id = "00000000-0000-0000-0000-000000000001"
    time_zone = "Europe/Copenhagen"
    cenc = calculate_cenc()
    print("In execute: Type of cenc: ", type(cenc))
    measurements = calculate_daily(
        cenc=cenc,
        internal_daily=internal_daily,
        time_zone=time_zone,
    )
    return (cenc, measurements)
