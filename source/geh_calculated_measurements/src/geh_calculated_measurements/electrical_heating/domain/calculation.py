from datetime import datetime

import pyspark.sql.functions as F
from geh_common.domain.types import MeteringPointType
from geh_common.pyspark.transformations import convert_to_utc
from geh_common.telemetry import use_span

import geh_calculated_measurements.electrical_heating.domain.transformations as trans
from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.domain.model import CalculatedMeasurementsDaily
from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    EphemeralColumnNames,
)
from geh_calculated_measurements.electrical_heating.domain.transformations.common import calculate_hourly_quantity


@use_span()
def execute(
    current_measurements: CurrentMeasurements,
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_zone: str,
    execution_start_datetime: datetime,
) -> CalculatedMeasurementsDaily:
    """Calculate the electrical heating for the given time series points and metering point periods.

    Returns the calculated electrical heating in UTC where the new value has changed.
    """
    # Join metering point periods and return them in local time and split by settlement month
    metering_point_periods = trans.get_joined_metering_point_periods_in_local_time(
        consumption_metering_point_periods, child_metering_points, time_zone, execution_start_datetime
    )

    # It's important that time series are aggregated hourly before converting to local time.
    # The reason is that when moving from DST to standard time, the same hour is duplicated in local time.
    time_series_points_hourly = calculate_hourly_quantity(current_measurements.df)

    # Add observation time in local time
    time_series_points_hourly = time_series_points_hourly.select(
        "*",
        F.from_utc_timestamp(F.col(EphemeralColumnNames.observation_time_hourly), time_zone).alias(
            EphemeralColumnNames.observation_time_hourly_lt
        ),
    )

    # Calculate electrical heating in local time
    new_electrical_heating = trans.calculate_electrical_heating_in_local_time(
        time_series_points_hourly, metering_point_periods, time_zone, execution_start_datetime
    )

    # Get old electrical heating in local time
    old_electrical_heating = trans.get_daily_energy_in_local_time(
        current_measurements.df, time_zone, [MeteringPointType.ELECTRICAL_HEATING]
    )

    # Filter out unchanged electrical heating
    changed_electrical_heating = trans.filter_unchanged_electrical_heating(
        new_electrical_heating, old_electrical_heating
    )

    changed_electrical_heating_in_utc = convert_to_utc(changed_electrical_heating, time_zone)

    return CalculatedMeasurementsDaily(changed_electrical_heating_in_utc)
