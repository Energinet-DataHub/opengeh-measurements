from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T
from geh_common.domain.types import MeteringPointType
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from geh_calculated_measurements.common.domain import ContractColumnNames
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
    """Calculate _calculated annual estimated net consumption_ (CENC) for metering points.."""
    # Constants for estimated consumption for move_in cases
    ESTIMATED_CONSUMPTION_MOVE_IN = 1800
    ESTIMATED_CONSUMPTION_MOVE_IN_WITH_HEATING = 5600

    # Filter and join the data
    filtered_time_series = _filter_relevant_time_series_points(time_series_points)
    parent_child_joined = _join_parent_child_with_consumption(
        child_metering_points, consumption_metering_point_periods, execution_start_datetime
    )

    # Add timestamp from settlement month
    parent_child_joined = _add_timestamp_from_settlement_month(parent_child_joined, execution_start_datetime, time_zone)

    # Extract net consumption points
    net_consumption_metering_points = _get_net_consumption_metering_points(parent_child_joined)

    # Process time series data
    metering_points_with_time_series = _join_and_sum_quantity(parent_child_joined, filtered_time_series)

    net_quantity = _calculate_net_quantity(metering_points_with_time_series)

    # Prepare final result with estimated values for move-in cases
    result = (
        net_consumption_metering_points.join(
            net_quantity,
            on=[ContractColumnNames.parent_metering_point_id, "settlement_date"],
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
        .select(
            F.col(ContractColumnNames.metering_point_id),
            F.col("net_quantity").alias(ContractColumnNames.quantity).cast(T.DecimalType(18, 3)),
            F.year(F.col("settlement_date")).alias("settlement_year"),
            F.month(F.col("settlement_date")).alias("settlement_month"),
        )
    )

    return Cenc(result)


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
) -> DataFrame:
    """Join child metering points with consumption metering point periods."""
    return (
        child_metering_points.df.alias("child")
        .join(
            consumption_metering_point_periods.df.alias("consumption"),
            F.col(f"child.{ContractColumnNames.parent_metering_point_id}")
            == F.col(f"consumption.{ContractColumnNames.metering_point_id}"),
            "left",
        )
        .filter(
            (F.lit(execution_start_datetime) >= F.col(f"consumption.{ContractColumnNames.period_from_date}"))
            & (
                F.col(f"consumption.{ContractColumnNames.period_to_date}").isNull()
                | (F.lit(execution_start_datetime) < F.col(f"consumption.{ContractColumnNames.period_to_date}"))
            )
        )
        .select(
            F.col(f"child.{ContractColumnNames.metering_point_id}"),
            F.col(f"child.{ContractColumnNames.metering_point_type}"),
            F.col(f"child.{ContractColumnNames.parent_metering_point_id}"),
            F.col(f"consumption.{ContractColumnNames.period_from_date}"),
            F.col(f"consumption.{ContractColumnNames.period_to_date}"),
            F.col(f"consumption.{ContractColumnNames.has_electrical_heating}"),
            F.col(f"consumption.{ContractColumnNames.settlement_month}"),
            F.col("consumption.move_in"),
        )
    )


def _add_timestamp_from_settlement_month(
    parent_child_joined: DataFrame,
    execution_start_datetime: datetime,
    time_zone: str,
) -> DataFrame:
    """Add settlement month timestamp."""
    return (
        parent_child_joined.withColumn(
            "utc_local_time", F.from_utc_timestamp(F.lit(execution_start_datetime), time_zone)
        )
        .withColumn(
            "settlement_date",
            F.make_date(
                F.when(
                    F.month(F.col("utc_local_time")) >= F.col(ContractColumnNames.settlement_month),
                    F.year(F.col("utc_local_time")),
                ).otherwise(F.year(F.col("utc_local_time")) - 1),
                F.col(ContractColumnNames.settlement_month),
                F.lit(1),
            ),
        )
        .drop("utc_local_time")
    )


def _get_net_consumption_metering_points(parent_child_df: DataFrame) -> DataFrame:
    """Extract net consumption metering points with most recent period for each point."""
    filtered_df = parent_child_df.filter(F.col("metering_point_type") == MeteringPointType.NET_CONSUMPTION.value)

    window_spec = Window.partitionBy(
        ContractColumnNames.metering_point_id, ContractColumnNames.parent_metering_point_id
    ).orderBy(F.col(ContractColumnNames.period_from_date).desc())

    return (
        filtered_df.withColumn("row_number", F.row_number().over(window_spec))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def _join_and_sum_quantity(parent_child_df: DataFrame, time_series_df: DataFrame) -> DataFrame:
    """Join metering point with time series and sum quantity for consumption_from_grid and supply_to_grid."""
    consumption_from_grid = MeteringPointType.CONSUMPTION_FROM_GRID.value
    supply_to_grid = MeteringPointType.SUPPLY_TO_GRID.value

    # Join and filter in one step
    joined_df = parent_child_df.join(
        time_series_df,
        on=[ContractColumnNames.metering_point_id, ContractColumnNames.metering_point_type],
        how="left",
    ).filter(
        F.col(ContractColumnNames.observation_time).between(
            F.add_months(F.col("settlement_date"), -12), F.col("settlement_date")
        )
    )

    # Calculate consumption and supply directly in one groupBy operation
    return joined_df.groupBy(ContractColumnNames.parent_metering_point_id, "settlement_date").agg(
        F.sum(
            F.when(
                F.col(ContractColumnNames.metering_point_type) == consumption_from_grid,
                F.col(ContractColumnNames.quantity),
            ).otherwise(0)
        ).alias(consumption_from_grid),
        F.sum(
            F.when(
                F.col(ContractColumnNames.metering_point_type) == supply_to_grid, F.col(ContractColumnNames.quantity)
            ).otherwise(0)
        ).alias(supply_to_grid),
    )


def _calculate_net_quantity(consumption_supply_df: DataFrame) -> DataFrame:
    """Calculate net quantity as maximum of (consumption - supply) or 0."""
    return consumption_supply_df.withColumn(
        "net_quantity",
        F.greatest(
            (
                F.col(str(MeteringPointType.CONSUMPTION_FROM_GRID.value))
                - F.col(str(MeteringPointType.SUPPLY_TO_GRID.value))
            ),
            F.lit(0),
        ),
    )
