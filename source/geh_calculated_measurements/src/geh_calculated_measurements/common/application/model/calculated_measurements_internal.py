import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain.column_names import ContractColumnNames

nullable = True


class CalculatedMeasurementsInternal(DataFrameWrapper):
    """The internal storage model of calculated measurements used by all calculations producing calculated measurements."""

    def __init__(self, df: DataFrame):
        super().__init__(
            df=df,
            schema=CalculatedMeasurementsInternal.schema,
            ignore_nullability=True,
        )

    schema = T.StructType(
        [
            #
            # "electrical_heating" or "capacity_settlement"
            T.StructField(ContractColumnNames.orchestration_type, T.StringType(), not nullable),
            #
            # ID of the orchestration that initiated the calculation job
            T.StructField(ContractColumnNames.orchestration_instance_id, T.StringType(), not nullable),
            #
            # Transaction ID. Created by the calculation job.
            # The ID refers to a continous set of measurements for a specific combination of orchestration_id and metering_point_id.
            # There are no time gaps for a given transaction id. Gaps introduces a new transaction ID after the gap.
            T.StructField(ContractColumnNames.transaction_id, T.StringType(), not nullable),
            #
            # A DateTime value indicating when the transaction was created
            # by the calculation job.
            T.StructField(ContractColumnNames.transaction_creation_datetime, T.TimestampType(), not nullable),
            #
            # A DateTime value indicating the first observation time for a transaction.
            T.StructField(ContractColumnNames.transaction_start_time, T.TimestampType(), not nullable),
            #
            # A DateTime value indicating the last observation time for a transaction.
            T.StructField(ContractColumnNames.transaction_end_time, T.TimestampType(), not nullable),
            #
            # Metering point ID
            T.StructField(ContractColumnNames.metering_point_id, T.StringType(), not nullable),
            #
            # "electrical_heating" | "capacity_settlement" | "net_consumption"
            T.StructField(ContractColumnNames.metering_point_type, T.StringType(), not nullable),
            #
            # UTC time
            T.StructField(ContractColumnNames.observation_time, T.TimestampType(), not nullable),
            # The calculated quantity
            T.StructField(ContractColumnNames.quantity, T.DecimalType(18, 3), not nullable),
            #
            # Settlement type
            # "up_to_end_of_period" | "end_of_period"
            T.StructField(ContractColumnNames.settlement_type, T.StringType(), nullable),
        ]
    )
