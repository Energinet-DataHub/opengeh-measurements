from geh_common.domain.types import MeteringPointType
from geh_common.pyspark.transformations import convert_from_utc
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.electrical_heating.domain.column_names import (
    ColumnNames,
)
from geh_calculated_measurements.electrical_heating.domain.transformations.common import (
    calculate_daily_quantity,
    calculate_hourly_quantity,
)


def get_daily_energy_in_local_time(
    time_series_points: DataFrame, time_zone: str, metering_point_types: list[MeteringPointType]
) -> DataFrame:
    energy = time_series_points.where(
        F.col(ColumnNames.metering_point_type).isin([t.value for t in metering_point_types])
    )
    energy = convert_from_utc(energy, time_zone)
    energy = calculate_daily_quantity(energy)

    return energy


def get_hourly_energy_in_local_time(
    time_series_points: DataFrame, time_zone: str, metering_point_types: list[MeteringPointType]
) -> DataFrame:
    energy = time_series_points.where(
        F.col(ColumnNames.metering_point_type).isin([t.value for t in metering_point_types])
    )
    energy = convert_from_utc(energy, time_zone)
    energy = calculate_hourly_quantity(energy)

    return energy
