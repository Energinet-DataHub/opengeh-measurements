import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame


class Cenc(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(df=df, schema=Cenc.schema, ignore_nullability=True)

    schema = T.StructType(
        [
            T.StructField("metering_point_id", T.StringType(), False),
            T.StructField("quantity", T.DecimalType(18, 3), False),
            T.StructField("settlement_year", T.IntegerType(), False),
            T.StructField("settlement_month", T.IntegerType(), False),
        ]
    )
