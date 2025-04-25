from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from geh_calculated_measurements.common.domain import ContractColumnNames

nullable = True


class MissingMeasurementsLog(DataFrameWrapper):
    def __init__(self, df: DataFrame) -> None:
        super().__init__(
            df,
            schema=self.schema,
            # We ignore_nullability because it has turned out to be too hard and even possibly
            # introducing more errors than solving in order to stay in exact sync with the
            # logically correct schema.
            ignore_nullability=True,
        )

    schema = T.StructType(
        [
            #
            # ID of the orchestration that initiated the calculation job
            T.StructField(ContractColumnNames.orchestration_instance_id, T.StringType(), not nullable),
            #
            T.StructField(ContractColumnNames.metering_point_id, T.StringType(), not nullable),
            #
            T.StructField(ContractColumnNames.date, T.TimestampType(), not nullable),
        ]
    )
