from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

from geh_calculated_measurements.electrical_heating.infrastructure.electricity_market.consumption_metering_point_periods.schema import (
    consumption_metering_point_periods_v1,
)


class ConsumptionMeteringPointPeriods(DataFrameWrapper):
    """Represents the consumption metering point periods data structure."""

    def __init__(self, df: DataFrame):
        super().__init__(
            df,
            consumption_metering_point_periods_v1,
            # We ignore_nullability because it has turned out to be too hard and even possibly
            # introducing more errors than solving in order to stay in exact sync with the
            # logically correct schema.
            ignore_nullability=True,
        )
