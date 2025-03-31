from geh_common.telemetry import use_span

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.missing_measurements_log.domain.model.metering_point_periods import (
    MeteringPointPeriods,
)


@use_span()
def execute(metering_point_periods: MeteringPointPeriods, current_measurements: CurrentMeasurements) -> None:
    metering_point_periods.df.show(n=20)  # TODO: Remove this line and do the actual calculation
    current_measurements.df.show(n=20)
