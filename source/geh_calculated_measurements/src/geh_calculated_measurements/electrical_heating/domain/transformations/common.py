from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ColumnNames
from geh_calculated_measurements.electrical_heating.domain.calculated_names import EphemiralNames


def calculate_hourly_quantity(time_series_points: DataFrame) -> DataFrame:
    """Use observation_time from input DataFrame and returns a DataFrame with observation_time_hourly."""
    hourly_window = Window.partitionBy(
        F.col(ColumnNames.metering_point_id), F.col(EphemiralNames.observation_time_hourly)
    )

    return (
        time_series_points.select(
            "*",
            F.date_trunc("hour", F.col(ColumnNames.observation_time)).alias(EphemiralNames.observation_time_hourly),
        )
        .select(
            F.sum(F.col(ColumnNames.quantity)).over(hourly_window).alias(ColumnNames.quantity),
            F.col(EphemiralNames.observation_time_hourly),
            F.col(ColumnNames.metering_point_id),
        )
        .drop_duplicates()
    )


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
