from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.electrical_heating.domain.ephemeral_column_names import EphemeralColumnNames


def calculate_hourly_quantity(time_series_points: DataFrame) -> DataFrame:
    """Use observation_time from input DataFrame and returns a DataFrame with observation_time_hourly."""
    return (
        time_series_points.where(F.col(ContractColumnNames.quantity).isNotNull())
        .withColumn(
            EphemeralColumnNames.observation_time_hourly,
            F.date_trunc("hour", F.col(ContractColumnNames.observation_time)),
        )
        .groupBy(ContractColumnNames.metering_point_id, EphemeralColumnNames.observation_time_hourly)
        .agg(F.sum(F.col(ContractColumnNames.quantity)).alias(ContractColumnNames.quantity))
        .select(
            ContractColumnNames.metering_point_id,
            EphemeralColumnNames.observation_time_hourly,
            ContractColumnNames.quantity,
        )
    )


def calculate_daily_quantity(time_series: DataFrame) -> DataFrame:
    return (
        time_series.withColumn(
            ContractColumnNames.date,
            F.date_trunc("day", F.col(ContractColumnNames.observation_time)),
        )
        .groupBy(ContractColumnNames.metering_point_id, ContractColumnNames.date)
        .agg(F.sum(F.col(ContractColumnNames.quantity)).alias(ContractColumnNames.quantity))
        .select(
            ContractColumnNames.metering_point_id,
            ContractColumnNames.date,
            ContractColumnNames.quantity,
        )
    )
