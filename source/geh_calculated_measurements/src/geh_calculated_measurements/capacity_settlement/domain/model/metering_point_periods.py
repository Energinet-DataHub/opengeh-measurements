from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from geh_calculated_measurements.common.domain import ColumnNames

nullable = True


class MeteringPointPeriods(DataFrameWrapper):
    """Represents the time series points data structure."""

    def __init__(self, df: DataFrame):
        super().__init__(
            df,
            metering_point_periods_v1,
            # We ignore_nullability because it has turned out to be too hard and even possibly
            # introducing more errors than solving in order to stay in exact sync with the
            # logically correct schema.
            ignore_nullability=True,
        )


metering_point_periods_v1 = T.StructType(
    [
        # ID of the consumption metering point (parent)
        T.StructField(ColumnNames.metering_point_id, T.StringType(), not nullable),
        #
        # Date when the consumption metering is either (a) entering 'connected'/'disconnected' first time or (b) move-in has occurred.
        # UTC time
        T.StructField(ColumnNames.period_from_date, T.TimestampType(), not nullable),
        #
        # Date when the consumption metering point is closed down or a move-in has occurred.
        # UTC time
        T.StructField(ColumnNames.period_to_date, T.TimestampType(), nullable),
        #
        # ID of the child metering point, which is of type 'capacity_settlement'
        T.StructField(ColumnNames.child_metering_point_id, T.StringType(), not nullable),
        #
        # The date where the child metering point (of type 'capacity_settlement') was created
        # UTC time
        T.StructField(ColumnNames.child_period_from_date, T.TimestampType(), not nullable),
        #
        # The date where the child metering point (of type 'capacity_settlement') was closed down
        # UTC time
        T.StructField(ColumnNames.child_period_to_date, T.TimestampType(), nullable),
    ]
)
