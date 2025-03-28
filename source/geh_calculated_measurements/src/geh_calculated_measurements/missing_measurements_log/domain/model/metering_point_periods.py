import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

nullable = True


class MeteringPointPeriods(DataFrameWrapper):
    """Represents periods for metering points with physical status "connected" or "disconnected".

    Includes all metering point types except those where subtype="calculated" or where type is "internal_use" (D99).
    The periods must be non-overlapping for a given metering point, but their timeline can be split into multiple rows/periods.
    """

    def __init__(self, df: DataFrame) -> None:
        super().__init__(
            df,
            schema=MeteringPointPeriods.schema,
            # We ignore_nullability because it has turned out to be too hard and even possibly
            # introducing more errors than solving in order to stay in exact sync with the
            # logically correct schema.
            ignore_nullability=True,
        )

    schema = T.StructType(
        [
            # GSRN number
            T.StructField("metering_point_id", T.StringType(), not nullable),
            #
            # The code of the grid area that the metering point belongs to
            T.StructField("grid_area_code", T.StringType(), not nullable),
            #
            # Metering point resolution: PT1H/PT15M
            T.StructField("resolution", T.StringType(), not nullable),
            #
            # UTC time
            T.StructField("period_from_date", T.TimestampType(), not nullable),
            #
            # UTC time
            T.StructField("period_to_date", T.TimestampType(), nullable),
        ]
    )
