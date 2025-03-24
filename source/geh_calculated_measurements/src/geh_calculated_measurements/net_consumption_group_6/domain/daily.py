from datetime import datetime
from decimal import Decimal

from geh_common.domain.types import MeteringPointType
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing

from geh_calculated_measurements.common.domain import CalculatedMeasurements
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
) -> CalculatedMeasurements:
    # TODO JMK: Replace this dummy code
    data = [
        (
            MeteringPointType.NET_CONSUMPTION.value,
            str(cenc.df.collect()[0].orchestration_instance_id),
            "ignored-transaction-id",
            datetime(2024, 12, 31, 23, 0, 0),
            "150000001500170200",
            MeteringPointType.NET_CONSUMPTION.value,
            datetime(2024, 12, 31, 23, 0, 0),
            Decimal("2.739"),
        )
    ]
    spark = initialize_spark()
    df = spark.createDataFrame(data=data, schema=CalculatedMeasurements.schema)
    return CalculatedMeasurements(df)
