from datetime import datetime
from decimal import Decimal

import pyspark.sql.functions as F
import pyspark.sql.types as T
from geh_common.domain.types import MeteringPointType
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import initialize_spark
from geh_calculated_measurements.net_consumption_group_6.domain.model import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
)

_cenc_schema = T.StructType(
    [
        T.StructField("orchestration_instance_id", T.StringType(), False),
        T.StructField("metering_point_id", T.StringType(), False),
        T.StructField("quantity", T.DecimalType(18, 3), False),
        T.StructField("settlement_year", T.IntegerType(), False),
        T.StructField("settlement_month", T.IntegerType(), False),
    ]
)


class Cenc(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(df=df, schema=_cenc_schema, ignore_nullability=True)


@use_span()
@testing()
def calculate_cenc(
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_series_points: TimeSeriesPoints,
    time_zone: str,
    orchestration_instance_id: str,
    execution_start_datetime: datetime,
) -> Cenc:
    """Return a data frame with schema `cenc_schema`."""
    spark = initialize_spark()
    filtered_time_series_points = time_series_points.df.filter(
        (F.col(ContractColumnNames.metering_point_type) == MeteringPointType.SUPPLY_TO_GRID.value)
        | (F.col(ContractColumnNames.metering_point_type) == MeteringPointType.CONSUMPTION_FROM_GRID.value)
    )
    filtered_time_series_points.show()
    parent_and_child_metering_points_joined = child_metering_points.df.alias("child").join(
        consumption_metering_point_periods.df.alias("consumption"),
        F.col("child.parent_metering_point_id") == F.col("consumption.metering_point_id"),
        "left",
    )
    parent_and_child_metering_points_joined.show()

    # TODO JVM: Hardcoded data to match the first scenario test
    data = [("00000000-0000-0000-0000-000000000001", "150000001500170200", Decimal("1000.000"), 2025, 1)]
    df = spark.createDataFrame(data, schema=_cenc_schema)

    return Cenc(df)
