from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from geh_calculated_measurements.common.domain import ContractColumnNames

nullable = True


class TimeSeriesPoints(DataFrameWrapper):
    """Represents the time series points data structure."""

    def __init__(self, df: DataFrame):
        super().__init__(
            df,
            time_series_points_v1,
            # We ignore_nullability because it has turned out to be too hard and even possibly
            # introducing more errors than solving in order to stay in exact sync with the
            # logically correct schema.
            ignore_nullability=True,
        )


time_series_points_v1 = T.StructType(
    [
        T.StructField(ContractColumnNames.metering_point_id, T.StringType(), not nullable),
        #
        # 'consumption' | 'capacity_settlement'
        T.StructField(ContractColumnNames.metering_point_type, T.StringType(), not nullable),
        #
        # UTC time
        T.StructField(
            ContractColumnNames.observation_time,
            T.TimestampType(),
            not nullable,
        ),
        #
        T.StructField(ContractColumnNames.quantity, T.DecimalType(18, 3), not nullable),
    ]
)
