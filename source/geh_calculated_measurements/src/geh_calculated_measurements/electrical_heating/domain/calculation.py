from uuid import UUID

from geh_common.pyspark.transformations import (
    convert_to_utc,
)
from geh_common.telemetry import use_span

import geh_calculated_measurements.electrical_heating.domain.transformations as T
from geh_calculated_measurements.electrical_heating.domain.calculated_measurements_daily import (
    CalculatedMeasurementsDaily,
)
from geh_calculated_measurements.electrical_heating.domain.calculated_measurements_factory import create
from geh_calculated_measurements.electrical_heating.infrastructure import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
)


@use_span()
def execute(
    time_series_points: TimeSeriesPoints,
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_zone: str,
) -> CalculatedMeasurementsDaily:
    """Calculate the electrical heating for the given time series points and metering point periods.

    Returns the calculated electrical heating in UTC where the new value has changed.
    """
    consumption_energy = T.get_daily_consumption_energy_in_local_time(time_series_points, time_zone)

    old_electrical_heating = T.get_electrical_heating_in_local_time(time_series_points, time_zone)

    metering_point_periods = T.get_joined_metering_point_periods_in_local_time(
        consumption_metering_point_periods, child_metering_points, time_zone
    )

    new_electrical_heating = T.calculate_electrical_heating_in_local_time(consumption_energy, metering_point_periods)

    changed_electrical_heating = T.filter_unchanged_electrical_heating(new_electrical_heating, old_electrical_heating)

    changed_electrical_heating_in_utc = convert_to_utc(changed_electrical_heating, time_zone)

    calculated_measurements = create(
        measurements=changed_electrical_heating_in_utc,
        orchestration_instance_id=UUID("00000000-0000-0000-0000-000000000000"),
        orchestration_type="orchestration_type",
        metering_point_type="MeteringPointType.ELECTRICAL",
    )

    return calculated_measurements
