from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T
from geh_common.domain.types import MeteringPointType
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.net_consumption_group_6.domain.model import (
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
    orchestration_instance_id: str,
    execution_start_datetime: datetime,
) -> Cenc:
    """Calculate net energy consumption (CENC) for metering points.

    Returns a DataFrame with schema `cenc_schema`.
    """
    # Constants for estimated consumption for move_in cases
    ESTIMATED_CONSUMPTION_MOVE_IN = 1800
    ESTIMATED_CONSUMPTION_MOVE_IN_WITH_HEATING = 5600

    # Filter and join the data
    filtered_time_series = _filter_relevant_time_series_points(time_series_points)
    parent_child_joined = _join_parent_child_with_consumption(
        child_metering_points, consumption_metering_point_periods, execution_start_datetime, time_zone
    )

    # Extract net consumption points
    net_consumption_points = _get_net_consumption_metering_points(parent_child_joined)

    # Process time series data
    joined_ts = _join_and_aggregate_time_series(parent_child_joined, filtered_time_series)
    consumption_supply = _calculate_consumption_supply(joined_ts)
    net_quantity = _calculate_net_quantity(consumption_supply)

    # Prepare final result with estimated values for move-in cases
    final_result = (
        net_consumption_points.join(
            net_quantity,
            on=[ContractColumnNames.parent_metering_point_id, "settlement_month_timestamp"],
            how="left",
        )
        .withColumn(
            "net_quantity",
            F.when(
                F.col("move_in") & F.col("has_electrical_heating"),
                F.lit(ESTIMATED_CONSUMPTION_MOVE_IN_WITH_HEATING),
            )
            .when(F.col("move_in"), F.lit(ESTIMATED_CONSUMPTION_MOVE_IN))
            .otherwise(F.col("net_quantity")),
        )
        .withColumn(ContractColumnNames.orchestration_instance_id, F.lit(orchestration_instance_id))
        .select(
            F.col(ContractColumnNames.orchestration_instance_id),
            F.col(ContractColumnNames.metering_point_id),
            F.col("net_quantity").alias(ContractColumnNames.quantity).cast(T.DecimalType(18, 3)),
            F.year(F.col("settlement_month_timestamp")).alias("settlement_year"),
            F.month(F.col("settlement_month_timestamp")).alias("settlement_month"),
        )
    )

    return Cenc(final_result)


def _filter_relevant_time_series_points(time_series_points: TimeSeriesPoints) -> DataFrame:
    """Filter time series points to include only supply_to_grid and consumption_from_grid."""
    return time_series_points.df.filter(
        F.col(ContractColumnNames.metering_point_type).isin(
            MeteringPointType.SUPPLY_TO_GRID.value, MeteringPointType.CONSUMPTION_FROM_GRID.value
        )
    )


def _join_parent_child_with_consumption(
    child_metering_points: ChildMeteringPoints,
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    execution_start_datetime: datetime,
    time_zone: str,
) -> DataFrame:
    """Join child metering points with consumption metering point periods and add settlement month."""
    return (
        child_metering_points.df.alias("child")
        .join(
            consumption_metering_point_periods.df.alias("consumption"),
            F.col(f"child.{ContractColumnNames.parent_metering_point_id}")
            == F.col(f"consumption.{ContractColumnNames.metering_point_id}"),
            "left",
        )
        .withColumn("utc_local_time", F.from_utc_timestamp(F.lit(execution_start_datetime), time_zone))
        .withColumn(
            "settlement_month_timestamp",
            F.to_timestamp(
                F.concat_ws(
                    "-",
                    F.when(
                        F.month(F.col("utc_local_time")) >= F.col(ContractColumnNames.settlement_month),
                        F.year(F.col("utc_local_time")),
                    ).otherwise(F.year(F.col("utc_local_time")) - 1),
                    F.col(ContractColumnNames.settlement_month),
                    F.lit(1),
                ),
                "yyyy-M-d",
            ),
        )
        .select(
            F.col(f"child.{ContractColumnNames.metering_point_id}"),
            F.col(f"child.{ContractColumnNames.metering_point_type}"),
            F.col("settlement_month_timestamp"),
            F.col(f"child.{ContractColumnNames.parent_metering_point_id}"),
            F.col(f"consumption.{ContractColumnNames.period_from_date}"),
            F.col(f"consumption.{ContractColumnNames.period_to_date}"),
            F.col(f"consumption.{ContractColumnNames.has_electrical_heating}"),
            F.col("consumption.move_in"),
        )
    )


def _get_net_consumption_metering_points(parent_child_df: DataFrame) -> DataFrame:
    """Extract net consumption metering points with most recent period for each point."""
    # Filter first to reduce data before window operation
    filtered_df = parent_child_df.filter(F.col("metering_point_type") == MeteringPointType.NET_CONSUMPTION.value)

    # Use more efficient window function
    window_spec = Window.partitionBy("metering_point_id", "parent_metering_point_id").orderBy(
        F.col("period_from_date").desc()
    )

    return (
        filtered_df.withColumn("row_number", F.row_number().over(window_spec))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def _join_and_aggregate_time_series(parent_child_df: DataFrame, time_series_df: DataFrame) -> DataFrame:
    """Join time series with metering points and aggregate quantities."""
    return (
        parent_child_df.join(
            time_series_df,
            on=[ContractColumnNames.metering_point_id, ContractColumnNames.metering_point_type],
            how="left",
        )
        .filter(
            F.col(ContractColumnNames.observation_time).between(
                F.add_months(F.col("settlement_month_timestamp"), -12), F.col("settlement_month_timestamp")
            )
        )
        .groupBy(
            ContractColumnNames.metering_point_id,
            ContractColumnNames.metering_point_type,
            "settlement_month_timestamp",
            ContractColumnNames.parent_metering_point_id,
        )
        .agg(F.sum(ContractColumnNames.quantity).alias(ContractColumnNames.quantity))
    )


def _calculate_consumption_supply(joined_ts_df: DataFrame) -> DataFrame:
    """Calculate consumption and supply without using pivot."""
    cons_type = MeteringPointType.CONSUMPTION_FROM_GRID.value
    supply_type = MeteringPointType.SUPPLY_TO_GRID.value

    # Filter and get consumption values
    consumption_df = (
        joined_ts_df.filter(F.col(ContractColumnNames.metering_point_type) == cons_type)
        .groupBy(ContractColumnNames.parent_metering_point_id, "settlement_month_timestamp")
        .agg(F.sum(ContractColumnNames.quantity).alias(cons_type))
    )

    # Filter and get supply values
    supply_df = (
        joined_ts_df.filter(F.col(ContractColumnNames.metering_point_type) == supply_type)
        .groupBy(ContractColumnNames.parent_metering_point_id, "settlement_month_timestamp")
        .agg(F.sum(ContractColumnNames.quantity).alias(supply_type))
    )

    # Join the two datasets
    return consumption_df.join(
        supply_df, on=[ContractColumnNames.parent_metering_point_id, "settlement_month_timestamp"], how="full_outer"
    )


def _calculate_net_quantity(consumption_supply_df: DataFrame) -> DataFrame:
    """Calculate net quantity as maximum of (consumption - supply) or 0."""
    return consumption_supply_df.withColumn(
        "net_quantity",
        F.greatest(
            F.coalesce(F.col(str(MeteringPointType.CONSUMPTION_FROM_GRID.value)), F.lit(0))
            - F.coalesce(F.col(str(MeteringPointType.SUPPLY_TO_GRID.value)), F.lit(0)),
            F.lit(0),
        ),
    )
