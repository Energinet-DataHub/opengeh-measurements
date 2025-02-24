import logging

from geh_common.pyspark.transformations import (
    convert_to_utc,
)
from geh_common.telemetry import use_span

import geh_calculated_measurements.electrical_heating.domain.transformations as T
from geh_calculated_measurements.electrical_heating.domain.calculated_measurements_daily import (
    CalculatedMeasurementsDaily,
)
from geh_calculated_measurements.electrical_heating.infrastructure import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
)

logging.basicConfig(level=logging.INFO)


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

    logging.info(f"number of rows in consumption_energy: {consumption_energy.count()}")

    old_electrical_heating = T.get_electrical_heating_in_local_time(time_series_points, time_zone)

    logging.info(f"number of rows in old_electrical_heating: {old_electrical_heating.count()}")

    metering_point_periods = T.get_joined_metering_point_periods_in_local_time(
        consumption_metering_point_periods, child_metering_points, time_zone
    )

    logging.info(f"number of rows in metering_point_periods: {metering_point_periods.count()}")

    new_electrical_heating = T.calculate_electrical_heating_in_local_time(consumption_energy, metering_point_periods)

    logging.info(f"number of rows in new_electrical_heating: {new_electrical_heating.count()}")

    changed_electrical_heating = T.filter_unchanged_electrical_heating(new_electrical_heating, old_electrical_heating)

    logging.info(f"number of rows in changed_electrical_heating: {changed_electrical_heating.count()}")

    changed_electrical_heating_in_utc = convert_to_utc(changed_electrical_heating, time_zone)

    logging.info(f"number of rows in changed_electrical_heating_in_utc: {changed_electrical_heating_in_utc.count()}")

    return CalculatedMeasurementsDaily(changed_electrical_heating_in_utc)
