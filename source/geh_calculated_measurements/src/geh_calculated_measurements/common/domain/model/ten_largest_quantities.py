import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

nullable = True


class TenLargestQuantities(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(
            df=df,
            schema=ten_largest_quantities_schema,
            ignore_nullability=True,
        )


ten_largest_quantities_schema = T.StructType(
    [
        #
        # ID of the orchestration that initiated the calculation job
        T.StructField("orchestration_instance_id", T.StringType(), not nullable),
        #
        # Metering point ID
        T.StructField("metering_point_id", T.StringType(), not nullable),
        #
        # UTC time
        T.StructField("observation_time", T.TimestampType(), not nullable),
        #
        # The calculated quantity
        T.StructField("quantity", T.DecimalType(18, 3), not nullable),
    ]
)
