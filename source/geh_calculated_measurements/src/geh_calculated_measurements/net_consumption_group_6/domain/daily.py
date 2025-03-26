from datetime import datetime
from decimal import Decimal

from geh_common.domain.types import MeteringPointType
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

from geh_calculated_measurements.common.infrastructure import initialize_spark
from geh_calculated_measurements.net_consumption_group_6.domain.cenc import Cenc
from geh_calculated_measurements.net_consumption_group_6.domain.model import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
)


@use_span()
@testing()
def calculate_daily(
    cenc: Cenc,
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_series_points: TimeSeriesPoints,
) -> DataFrame:
    # TODO JMK: Replace this dummy code
    data = [
        (
            "150000001500170200",
            MeteringPointType.NET_CONSUMPTION.value,
            datetime(2024, 12, 31, 23, 0, 0),
            Decimal("2.739"),
        )
    ]
    spark = initialize_spark()
    schema = StructType(
        [
            StructField("metering_point_id", StringType(), True),
            StructField("metering_point_type", StringType(), True),
            StructField("date", TimestampType(), True),
            StructField("quantity", DecimalType(18, 3), True),
        ]
    )
    df = spark.createDataFrame(data, schema=schema)
    return df
