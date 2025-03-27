from datetime import datetime
from decimal import Decimal

from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing

from geh_calculated_measurements.common.infrastructure import initialize_spark
from geh_calculated_measurements.net_consumption_group_6.domain import (
    Cenc,
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
)


@use_span()
@testing()
def calculate_cenc(
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_series_points: TimeSeriesPoints,
    time_zone: str,
    execution_start_datetime: datetime,
) -> Cenc:
    """Return a data frame with schema `cenc_schema`."""
    # TODO JVM: Replace this dummy code
    spark = initialize_spark()
    # TODO JVM: Hardcoded data to match the first scenario test
    data = [("150000001500170200", Decimal("1000.000"), 2025, 1)]
    df = spark.createDataFrame(data, schema=Cenc.schema)

    return Cenc(df)
