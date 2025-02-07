from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark_functions.functions import convert_from_utc

from opengeh_electrical_heating.domain.column_names import ColumnNames
from opengeh_electrical_heating.domain.transformations.common import calculate_daily_quantity
from opengeh_electrical_heating.domain.types.metering_point_type import MeteringPointType


def get_daily_consumption_energy_in_local_time(time_series_points: DataFrame, time_zone: str) -> DataFrame:
    consumption_energy = time_series_points.where(
        (F.col(ColumnNames.metering_point_type) == MeteringPointType.CONSUMPTION_METERING_POINT_TYPE.value)
        | (F.col(ColumnNames.metering_point_type) == MeteringPointType.NET_CONSUMPTION.value)
    )
    consumption_energy = convert_from_utc(consumption_energy, time_zone)
    consumption_energy = calculate_daily_quantity(consumption_energy)

    consumption_energy.printSchema()
    return consumption_energy
