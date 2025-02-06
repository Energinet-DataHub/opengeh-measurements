from pyspark.sql import DataFrame
from pyspark_functions.data_frame_wrapper import DataFrameWrapper


class CalculatedMeasurements(DataFrameWrapper):
    """Represents the calculated measurements data structure."""

    def __init__(self, df: DataFrame):
        super().__init__(
            df,
            calculated_measurements_v1,
            # We ignore_nullability because it has turned out to be too hard and even possibly
            # introducing more errors than solving in order to stay in exact sync with the
            # logically correct schema.
            ignore_nullability=True,
        )
