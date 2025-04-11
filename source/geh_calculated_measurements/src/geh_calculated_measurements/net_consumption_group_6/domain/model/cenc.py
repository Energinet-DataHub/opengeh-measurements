import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import ContractColumnNames


class Cenc(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(df=df, schema=Cenc.schema, ignore_nullability=True)

    schema = T.StructType(
        [
            T.StructField(ContractColumnNames.metering_point_id, T.StringType(), False),
            T.StructField(ContractColumnNames.quantity, T.DecimalType(18, 3), False),
            T.StructField(ContractColumnNames.settlement_year, T.IntegerType(), False),
            T.StructField(ContractColumnNames.settlement_month, T.IntegerType(), False),
        ]
    )
