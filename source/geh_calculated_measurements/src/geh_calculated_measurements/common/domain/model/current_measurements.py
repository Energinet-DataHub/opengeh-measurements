import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

nullable = True


class CurrentMeasurements(DataFrameWrapper):
    """All current (latest) measurements. This is a generic type used for multiple types of calculation."""

    schema = T.StructType(
        [
            # GSRN (18 characters) that uniquely identifies the metering point
            # Example: 578710000000000103
            T.StructField("metering_point_id", T.StringType(), not nullable),
            #
            # Example: consumption, production, electrical_heating etc.
            T.StructField("metering_point_type", T.StringType(), not nullable),
            #
            # Energy quantity in kWh for the given observation time.
            # Example: 1234.534
            T.StructField("quantity", T.DecimalType(18, 3), not nullable),
            #
            #  "missing" | "estimated" | "measured" | "calculated"
            # Example: measured
            T.StructField("quality", T.StringType(), not nullable),
            #
            # UTC time
            T.StructField("observation_time", T.TimestampType(), not nullable),
        ]
    )

    def __init__(self, df: DataFrame):
        super().__init__(
            df=df,
            schema=self.schema,
            ignore_nullability=True,
        )
