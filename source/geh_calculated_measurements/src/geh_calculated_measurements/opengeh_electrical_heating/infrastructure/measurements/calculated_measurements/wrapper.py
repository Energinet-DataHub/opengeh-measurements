from pyspark.sql import DataFrame
from pyspark_functions.data_frame_wrapper import DataFrameWrapper

from opengeh_electrical_heating.infrastructure.measurements.calculated_measurements.schema import (
    calculated_measurements_schema,
)


class CalculatedMeasurements(DataFrameWrapper):
    """Represents the calculated measurements data structure."""

    def __init__(self, df: DataFrame):
        super().__init__(
            df,
            calculated_measurements_schema,
            # We ignore_nullability because it has turned out to be too hard and even possibly
            # introducing more errors than solving in order to stay in exact sync with the
            # logically correct schema.
            ignore_nullability=True,
        )
