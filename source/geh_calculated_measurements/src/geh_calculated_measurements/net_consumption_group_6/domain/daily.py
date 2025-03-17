import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import DataFrame

from geh_calculated_measurements.net_consumption_group_6.domain.cenc import Cenc

_daily_schema = T.StructType(
    [
        T.StructField("orchestration_instance_id", T.StringType(), False),
        T.StructField("metering_point_id", T.StringType(), False),
        T.StructField("quantity", T.DecimalType(18, 3), False),
        T.StructField("observation_date", T.TimestampType(), False),
    ]
)


class Daily(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(df=df, schema=_daily_schema, ignore_nullability=True)


@use_span()
@testing()
def calculate_daily(cenc: Cenc) -> Daily:
    # Replace this dummy code
    daily = cenc.df

    return Daily(daily)
