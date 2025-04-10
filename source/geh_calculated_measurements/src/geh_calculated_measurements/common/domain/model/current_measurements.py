from geh_common.data_products.measurements_core.measurements_gold.current_v1 import schema
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

nullable = True


class CurrentMeasurements(DataFrameWrapper):
    """All current (latest) measurements. This is a generic type used for multiple types of calculation."""

    schema = schema

    def __init__(self, df: DataFrame):
        super().__init__(
            df=df,
            schema=self.schema,
            ignore_nullability=True,
        )
