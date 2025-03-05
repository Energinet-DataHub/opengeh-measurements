from uuid import UUID

import pyspark.sql.functions as F
from geh_common.domain.types import MeteringPointType, OrchestrationType
from geh_common.pyspark.transformations import (
    convert_to_utc,
)
from geh_common.telemetry import use_span

import geh_calculated_measurements.electrical_heating.domain.transformations as T
from geh_calculated_measurements.common.domain import (
    CalculatedMeasurements,
    calculated_measurements_factory,
)
from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    EphemiralColumnNames,
    TimeSeriesPoints,
)
from geh_calculated_measurements.electrical_heating.domain.transformations.common import calculate_hourly_quantity


@use_span()
def execute(
    time_series_points: TimeSeriesPoints,
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_zone: str,
    orchestration_instance_id: UUID,
) -> CalculatedMeasurements:
    """Calculate the electrical heating for the given time series points and metering point periods.

    Returns the calculated electrical heating in UTC where the new value has changed.
    """
    # The periods are in local time and are split by year
    metering_point_periods = T.get_joined_metering_point_periods_in_local_time(
        consumption_metering_point_periods, child_metering_points, time_zone
    )

    # It's important that time series are aggregated hourly before converting to local time.
    # The reason is that when moving from DST to standard time, the same hour is repeated in local time.
    time_series_points_hourly = calculate_hourly_quantity(time_series_points.df)
    time_series_points_hourly = time_series_points_hourly.withColumn(
        EphemiralColumnNames.observation_time_hourly_lt,
        F.from_utc_timestamp(F.col(EphemiralColumnNames.observation_time_hourly), time_zone),
    )
    new_electrical_heating = T.calculate_electrical_heating_in_local_time(
        time_series_points_hourly, metering_point_periods
    )

    old_electrical_heating = T.get_daily_energy_in_local_time(
        time_series_points.df, time_zone, [MeteringPointType.ELECTRICAL_HEATING]
    )
    changed_electrical_heating = T.filter_unchanged_electrical_heating(new_electrical_heating, old_electrical_heating)

    changed_electrical_heating_in_utc = convert_to_utc(changed_electrical_heating, time_zone)

    calculated_measurements = calculated_measurements_factory.create(
        measurements=changed_electrical_heating_in_utc,
        orchestration_instance_id=orchestration_instance_id,
        orchestration_type=OrchestrationType.ELECTRICAL_HEATING,
        metering_point_type=MeteringPointType.ELECTRICAL_HEATING,
        time_zone=time_zone,
    )

    return calculated_measurements
