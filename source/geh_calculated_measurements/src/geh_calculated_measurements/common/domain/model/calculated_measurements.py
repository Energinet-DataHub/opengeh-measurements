from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from geh_calculated_measurements.common.domain.column_names import ContractColumnNames


class CalculatedMeasurementsDaily(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(
            df=df,
            schema=self.schema,
            ignore_nullability=True,
        )

    schema = T.StructType(
        [
            T.StructField(ContractColumnNames.metering_point_id, T.StringType(), False),
            T.StructField(ContractColumnNames.date, T.TimestampType(), False),
            T.StructField(ContractColumnNames.quantity, T.DecimalType(18, 3), False),
        ]
    )
