from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

from geh_calculated_measurements.electrical_heating.infrastructure.measurements.calculated_measurements.schema import (
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
