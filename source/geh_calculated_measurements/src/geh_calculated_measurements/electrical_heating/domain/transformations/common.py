from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from geh_calculated_measurements.electrical_heating.domain.column_names import ColumnNames


def calculate_hourly_quantity(time_series_points: DataFrame) -> DataFrame:
    """Use observation_time from input DataFrame and returns a DataFrame with observation_time_hourly."""
    hourly_window = Window.partitionBy(
        F.col(ColumnNames.metering_point_id), F.col(CalculatedNames.observation_time_hourly)
    )

    return (
        time_series_points.select(
            "*",
            F.date_trunc("hour", F.col(ColumnNames.observation_time)).alias(CalculatedNames.observation_time_hourly),
        )
        .select(
            F.sum(F.col(ColumnNames.quantity)).over(hourly_window).alias(ColumnNames.quantity),
            F.col(CalculatedNames.observation_time_hourly),
            F.col(ColumnNames.metering_point_id),
        )
        .drop_duplicates()
    )


def calculate_daily_quantity(time_series_points: DataFrame) -> DataFrame:
    # TODO: Can this be rewritten as
    # return (
    #     time_series_points
    #     .withColumn(CalculatedNames.date, F.date_trunc("day", F.col(ColumnNames.observation_time)))
    #     .groupBy(ColumnNames.metering_point_id, CalculatedNames.date)
    #     .agg(F.sum(F.col(ColumnNames.quantity)).alias(ColumnNames.quantity))
    # )

    daily_window = Window.partitionBy(
        F.col(ColumnNames.metering_point_id),
        F.col(ColumnNames.date),
    )

    return (
        time_series_points.select(
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
