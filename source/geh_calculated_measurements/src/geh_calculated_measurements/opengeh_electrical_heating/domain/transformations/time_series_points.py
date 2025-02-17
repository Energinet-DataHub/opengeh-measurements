from geh_common.domain.types import MeteringPointType
from geh_common.pyspark.transformations import convert_from_utc
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.opengeh_electrical_heating.domain.column_names import (
    ColumnNames,
)
from geh_calculated_measurements.opengeh_electrical_heating.domain.transformations.common import (
    calculate_daily_quantity,
)
from geh_calculated_measurements.opengeh_electrical_heating.infrastructure import (
    TimeSeriesPoints,
)


def get_daily_consumption_energy_in_local_time(time_series_points: TimeSeriesPoints, time_zone: str) -> DataFrame:
    consumption_energy = time_series_points.df.where(
        (F.col(ColumnNames.metering_point_type) == MeteringPointType.CONSUMPTION_METERING_POINT_TYPE.value)
        | (F.col(ColumnNames.metering_point_type) == MeteringPointType.NET_CONSUMPTION.value)
    )
    consumption_energy = convert_from_utc(consumption_energy, time_zone)
    consumption_energy = calculate_daily_quantity(consumption_energy)

    return consumption_energy


def get_electrical_heating_in_local_time(time_series_points: TimeSeriesPoints, time_zone: str) -> DataFrame:
    old_electrical_heating = time_series_points.df.where(
        F.col(ColumnNames.metering_point_type) == MeteringPointType.ELECTRICAL_HEATING.value
    )
    old_electrical_heating = convert_from_utc(old_electrical_heating, time_zone)
    old_electrical_heating = calculate_daily_quantity(old_electrical_heating)
    return old_electrical_heating
