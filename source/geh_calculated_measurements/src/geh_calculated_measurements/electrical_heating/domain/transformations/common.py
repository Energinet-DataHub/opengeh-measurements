from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.electrical_heating.domain.ephemiral_column_names import EphemiralColumnNames


def calculate_hourly_quantity(time_series_points: DataFrame) -> DataFrame:
    """Use observation_time from input DataFrame and returns a DataFrame with observation_time_hourly."""
    hourly_window = Window.partitionBy(
        F.col(ContractColumnNames.metering_point_id), F.col(EphemiralColumnNames.observation_time_hourly)
    )

    return (
        time_series_points.select(
            "*",
            F.date_trunc("hour", F.col(ContractColumnNames.observation_time)).alias(
                EphemiralColumnNames.observation_time_hourly
            ),
        )
        .select(
            F.sum(F.col(ContractColumnNames.quantity)).over(hourly_window).alias(ContractColumnNames.quantity),
            F.col(EphemiralColumnNames.observation_time_hourly),
            F.col(ContractColumnNames.metering_point_id),
        )
        .drop_duplicates()
    )


def calculate_daily_quantity(time_series: DataFrame) -> DataFrame:
    daily_window = Window.partitionBy(
        F.col(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.date),
    )

    return (
        time_series.select(
            "*",
            F.date_trunc("day", F.col(ContractColumnNames.observation_time)).alias(ContractColumnNames.date),
        )
        .select(
            F.sum(F.col(ContractColumnNames.quantity)).over(daily_window).alias(ContractColumnNames.quantity),
            F.col(ContractColumnNames.date),
            F.col(ContractColumnNames.metering_point_id),
        )
        .drop_duplicates()
    )
