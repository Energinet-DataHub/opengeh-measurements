from geh_common.data_products.electricity_market_measurements_input.electrical_heating_child_metering_points_v1 import (
    schema,
)
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame


class ChildMeteringPoints(DataFrameWrapper):
    """Represents the child metering points data structure."""

    def __init__(self, df: DataFrame) -> None:
        super().__init__(
            df,
            ChildMeteringPoints.schema,
            # We ignore_nullability because it has turned out to be too hard and even possibly
            # introducing more errors than solving in order to stay in exact sync with the
            # logically correct schema.
            ignore_nullability=True,
        )

    schema = schema
