from pyspark.sql import DataFrame
from pyspark_functions.functions import (
    convert_to_utc,
)
from telemetry_logging import use_span

import opengeh_electrical_heating.domain.transformations as T
from opengeh_electrical_heating.domain.calculated_measurements_daily import CalculatedMeasurementsDaily


@use_span()
def execute(
    time_series_points: DataFrame,
    consumption_metering_point_periods: DataFrame,
    child_metering_points: DataFrame,
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

    return CalculatedMeasurementsDaily(changed_electrical_heating_in_utc)
