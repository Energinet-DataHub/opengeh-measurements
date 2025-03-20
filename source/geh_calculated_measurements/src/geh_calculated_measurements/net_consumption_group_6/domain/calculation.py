from typing import Tuple

from geh_common.telemetry import use_span
from pyspark.pandas import DataFrame

from geh_calculated_measurements.common.domain import CalculatedMeasurements
from geh_calculated_measurements.net_consumption_group_6.domain.cenc import Cenc, calculate_cenc
from geh_calculated_measurements.net_consumption_group_6.domain.daily import calculate_daily


@use_span()
def execute(
    internal_daily: DataFrame,
) -> Tuple[Cenc, CalculatedMeasurements]:
    orchestration_instance_id = "00000000-0000-0000-0000-000000000001"
    calculation_day = 1
    calculation_month = 1
    calculation_year = 2025
    time_zone = "Europe/Copenhagen"
    cenc = calculate_cenc()
    measurements = calculate_daily(
        internal_daily,
        cenc,
        orchestration_instance_id,
        calculation_day,
        calculation_month,
        calculation_year,
        time_zone,
    )
    return (cenc, measurements)
