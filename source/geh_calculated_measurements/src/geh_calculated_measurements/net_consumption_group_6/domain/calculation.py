from typing import Tuple

from geh_common.telemetry import use_span

from geh_calculated_measurements.net_consumption_group_6.domain.cenc import Cenc, calculate_cenc
from geh_calculated_measurements.net_consumption_group_6.domain.daily import Daily, calculate_daily


@use_span()
def execute() -> Tuple[Cenc, Daily]:
    cenc = calculate_cenc()
    daily = calculate_daily(cenc)
    return (cenc, daily)
