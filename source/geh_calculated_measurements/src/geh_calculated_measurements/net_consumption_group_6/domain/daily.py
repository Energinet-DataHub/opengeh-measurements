import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import DataFrame

from geh_calculated_measurements.net_consumption_group_6.domain.cenc import Cenc
from geh_calculated_measurements.net_consumption_group_6.domain.model import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
)

_daily_schema = T.StructType(
    [
        T.StructField("orchestration_instance_id", T.StringType(), False),
        T.StructField("metering_point_id", T.StringType(), False),
        T.StructField("quantity", T.DecimalType(18, 3), False),
        T.StructField("observation_date", T.TimestampType(), False),
    ]
)


class Daily(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(df=df, schema=_daily_schema, ignore_nullability=True)


@use_span()
@testing()
def calculate_daily(
    cenc: Cenc,
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_series_points: TimeSeriesPoints,
) -> Daily:
    # TODO BJM: Replace this dummy code
    daily = cenc.df

    return Daily(daily)
