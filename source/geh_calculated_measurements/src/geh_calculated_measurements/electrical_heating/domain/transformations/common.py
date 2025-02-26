from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ColumnNames


def calculate_daily_quantity(time_series: DataFrame) -> DataFrame:
    daily_window = Window.partitionBy(
        F.col(ColumnNames.metering_point_id),
        F.col(ColumnNames.date),
    )

    return (
        time_series.select(
            "*",
            F.date_trunc("day", F.col(ColumnNames.observation_time)).alias(ColumnNames.date),
        )
        .select(
            F.sum(F.col(ColumnNames.quantity)).over(daily_window).alias(ColumnNames.quantity),
            F.col(ColumnNames.date),
            F.col(ColumnNames.metering_point_id),
        )
        .drop_duplicates()
    )
