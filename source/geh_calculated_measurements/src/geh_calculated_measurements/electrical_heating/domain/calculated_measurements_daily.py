import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

nullable = True


class CalculatedMeasurementsDaily(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(
            df=df,
            schema=calculated_measurements_daily_schema,
            ignore_nullability=True,
        )


calculated_measurements_daily_schema = T.StructType(
    [
        #
        # "electrical_heating" or "capacity_settlement"
        T.StructField("orchestration_type", T.StringType(), not nullable),
        #
        # ID of the orchestration that initiated the calculation job
        T.StructField("orchestration_instance_id", T.StringType(), not nullable),
        #
        # Metering point ID
        T.StructField("metering_point_id", T.StringType(), not nullable),
        #
        # Transaction ID. Created by the calculation job.
        # The ID refers to a continous set of measurements for a specific combination of orchestration_id and metering_point_id.
        # There are no time gaps for a given transaction id. Gaps introduces a new transaction ID after the gap.
        T.StructField("transaction_id", T.StringType(), not nullable),
        #
        # A DateTime value indicating when the transaction was created
        # by the calculation job.
        T.StructField("transaction_creation_datetime", T.TimestampType(), not nullable),
        #
        # "electrical_heating" or "capacity_settlement"
        T.StructField("metering_point_type", T.StringType(), not nullable),
        #
        # UTC time
        T.StructField(
            "date",
            T.TimestampType(),
            not nullable,
        ),
        # The calculated quantity
        T.StructField("quantity", T.DecimalType(18, 3), not nullable),
    ]
)
