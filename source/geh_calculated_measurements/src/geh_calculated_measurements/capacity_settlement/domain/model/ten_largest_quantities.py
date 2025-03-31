import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain.column_names import ContractColumnNames

nullable = True


class TenLargestQuantities(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(
            df=df,
            schema=TenLargestQuantities.schema,
            ignore_nullability=True,
        )

    schema = T.StructType(
        [
            #
            # Metering point ID
            T.StructField(ContractColumnNames.metering_point_id, T.StringType(), not nullable),
            #
            # UTC time
            T.StructField(ContractColumnNames.observation_time, T.TimestampType(), not nullable),
            #
            # The calculated quantity
            T.StructField(ContractColumnNames.quantity, T.DecimalType(18, 3), not nullable),
        ]
    )
