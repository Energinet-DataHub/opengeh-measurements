from geh_common.pyspark.transformations import convert_from_utc
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.opengeh_electrical_heating.domain.column_names import (
    ColumnNames,
)
from geh_calculated_measurements.opengeh_electrical_heating.domain.transformations.common import (
    calculate_daily_quantity,
)
from geh_calculated_measurements.opengeh_electrical_heating.domain.types.metering_point_type import MeteringPointType


def get_daily_energy_in_local_time(
    time_series_points: DataFrame, time_zone: str, metering_point_types: list[MeteringPointType]
) -> DataFrame:
    consumption_energy = time_series_points.where(
        F.col(ColumnNames.metering_point_type).isin([t.value for t in metering_point_types])
    )
    consumption_energy = convert_from_utc(consumption_energy, time_zone)
    consumption_energy = calculate_daily_quantity(consumption_energy)

    return consumption_energy
