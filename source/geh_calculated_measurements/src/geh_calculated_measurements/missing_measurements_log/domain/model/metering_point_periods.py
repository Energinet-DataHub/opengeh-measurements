from geh_common.data_products.electricity_market_measurements_input.missing_measurements_log_metering_point_periods_v1 import (
    schema,
)
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
            schema=schema,
            # We ignore_nullability because it has turned out to be too hard and even possibly
            # introducing more errors than solving in order to stay in exact sync with the
            # logically correct schema.
            ignore_nullability=True,
        )

    schema = schema
