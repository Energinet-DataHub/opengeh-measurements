from datetime import datetime
from typing import Tuple

import pyspark.sql.functions as F
import pyspark.sql.types as T
from geh_common.domain.types import MeteringPointType
from geh_common.pyspark.transformations import convert_from_utc, convert_to_utc
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import DataFrame, Window

from geh_calculated_measurements.common.domain import ContractColumnNames, CurrentMeasurements
from geh_calculated_measurements.net_consumption_group_6.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)


@use_span()
@testing()
def cnc(
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    current_measurements: CurrentMeasurements,
    time_zone: str,
    execution_start_datetime: datetime,
) -> Tuple[DataFrame, DataFrame]:
    """Execute the calculation of net consumption for a group of metering points.

    Parameters:
    consumption_metering_point_periods : ConsumptionMeteringPointPeriods
      Information about consumption metering point periods.
    child_metering_points : ChildMeteringPoints
      Information about child metering points.
    current_measurements : CurrentMeasurements
      Current measurement data for metering points.
    time_zone : str
      The time zone to use for datetime conversions.
    execution_start_datetime : datetime
      The start time for the execution process.

    Returns:
    Tuple[DataFrame, DataFrame]
      A tuple containing:
      - DataFrame with periods and their calculated net consumption (converted to UTC)
      - DataFrame with periods and their corresponding time series data (converted to UTC)
    """
    filtered_time_series = _filter_and_aggregate_daily(current_measurements)

    parent_child_joined = _join_child_to_consumption(
        consumption_metering_point_periods, child_metering_points, execution_start_datetime
    )
    filtered_time_series = convert_from_utc(filtered_time_series, time_zone)
    parent_child_joined = convert_from_utc(parent_child_joined, time_zone)

    parent_child_joined = close_open_period(parent_child_joined)

    metering_points = _filter_periods_by_cut_off(parent_child_joined)

    periods = _split_periods_by_settlement_month(metering_points)

    periods_with_cut_off = _determin_cut_off_for_periods(periods)

    periods_with_ts = _join_metering_point_periods_to_time_series(filtered_time_series, periods)

    net_consumption_over_ts = _sum_supply_and_consumption(periods_with_ts)

    periods_with_net_consumption = _create_daily_quantity_per_period(periods_with_cut_off, net_consumption_over_ts)

    return (
        convert_to_utc(periods_with_net_consumption, time_zone),
        convert_to_utc(periods_with_ts, time_zone),
    )


def _filter_and_aggregate_daily(current_measurements: CurrentMeasurements) -> DataFrame:
    """Filter and aggregate daily measurements.

    This function filters the current measurements data and aggregates it to daily observations.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - metering_point_type: Type of the metering point
            - date: Daily observation time
            - quantity: The quantity as DecimalType(18, 3)
    """
    filtered_time_series = current_measurements.df.where(
        F.col(ContractColumnNames.metering_point_type).isin(
            MeteringPointType.SUPPLY_TO_GRID.value, MeteringPointType.CONSUMPTION_FROM_GRID.value
        )
    )
    filtered_time_series_daily = (
        filtered_time_series.select(
            "*",
            F.to_date(F.col(ContractColumnNames.observation_time)).alias(ContractColumnNames.date),
        )
        .groupBy(
            ContractColumnNames.metering_point_id,
            ContractColumnNames.metering_point_type,
            ContractColumnNames.date,
        )
        .agg(
            F.sum(ContractColumnNames.quantity).alias(ContractColumnNames.quantity),
        )
        .select(
            F.col(ContractColumnNames.metering_point_id),
            F.col(ContractColumnNames.metering_point_type),
            F.col(ContractColumnNames.date),
            F.col(ContractColumnNames.quantity).cast(T.DecimalType(18, 3)),
        )
    )

    return filtered_time_series_daily


def _join_child_to_consumption(
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    execution_start_datetime: datetime,
) -> DataFrame:
    """Join child metering points with consumption metering point periods.

    This function joins child metering points with consumption metering point periods.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - metering_point_type: Type of the metering point
            - parent_metering_point_id: ID of the parent metering point
            - period_from_date: Start date of the period
            - period_to_date: End date of the period
            - settlement_month: Settlement month
            - execution_start_datetime: Execution start date and time
    """
    parent_child_joined = (
        child_metering_points.df.alias("child")
        .join(
            consumption_metering_point_periods.df.alias("consumption"),
            F.col(f"child.{ContractColumnNames.parent_metering_point_id}")
            == F.col(f"consumption.{ContractColumnNames.metering_point_id}"),
            "left",
        )
        .select(
            F.col(f"child.{ContractColumnNames.metering_point_id}"),
            F.col(f"child.{ContractColumnNames.metering_point_type}"),
            F.col(f"child.{ContractColumnNames.parent_metering_point_id}"),
            F.col(f"consumption.{ContractColumnNames.period_from_date}"),
            F.col(f"consumption.{ContractColumnNames.period_to_date}"),
            F.col(f"consumption.{ContractColumnNames.settlement_month}"),
            F.lit(execution_start_datetime).cast(T.TimestampType()).alias("execution_start_datetime"),
        )
    )

    return parent_child_joined


def close_open_period(parent_child_joined: DataFrame) -> DataFrame:
    """Close open periods in the parent-child joined DataFrame.

    This function closes open periods by setting the period_to_date to the execution_start_datetime

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - metering_point_type: Type of the metering point
            - parent_metering_point_id: ID of the parent metering point
            - period_from_date: Start date of the period
            - period_to_date: End date of the period
            - settlement_month: Settlement month
            - execution_start_datetime: Execution start date and time
            - period_to_date: End date of the period
    """
    return parent_child_joined.select(
        F.col(f"{ContractColumnNames.metering_point_id}"),
        F.col(f"{ContractColumnNames.metering_point_type}"),
        F.col(f"{ContractColumnNames.parent_metering_point_id}"),
        F.col(f"{ContractColumnNames.period_from_date}"),
        F.col(f"{ContractColumnNames.settlement_month}"),
        F.col("execution_start_datetime"),
        F.when(
            F.col(ContractColumnNames.period_to_date).isNull(),
            F.when(
                F.month(F.col("execution_start_datetime")) >= F.col(ContractColumnNames.settlement_month),
                F.make_date(
                    F.year(F.col("execution_start_datetime")),
                    F.col(ContractColumnNames.settlement_month),
                    F.lit(1),
                ),
            ).otherwise(
                F.make_date(
                    F.year(F.col("execution_start_datetime")) - F.lit(1),
                    F.col(ContractColumnNames.settlement_month),
                    F.lit(1),
                )
            ),
        )
        .otherwise(F.col(ContractColumnNames.period_to_date))
        .alias(ContractColumnNames.period_to_date),
    )


def _filter_periods_by_cut_off(parent_child_joined: DataFrame) -> DataFrame:
    """Generate metering points with cut off date.

    This function generates metering points with a cut off date.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - metering_point_type: Type of the metering point
            - parent_metering_point_id: ID of the parent metering point
            - period_from_date: Start date of the period
            - period_to_date: End date of the period
            - settlement_month: Settlement month
            - execution_start_datetime: Execution start date and time
            - cut_off_date: Cut off date
    """
    THREE_YEARS_IN_MONTHS = 3 * 12
    metering_points = (
        parent_child_joined.select(
            "*",
            F.add_months(
                F.make_date(
                    F.year(F.col("execution_start_datetime")),
                    F.month(F.col("execution_start_datetime")),
                    F.day(F.col("execution_start_datetime")),
                ),
                -THREE_YEARS_IN_MONTHS,
            ).alias("cut_off_date"),
        )
        .where(
            F.col(ContractColumnNames.period_to_date).isNotNull()
            | (F.col(ContractColumnNames.period_to_date) <= F.col("cut_off_date"))
        )
        .select(
            F.col(ContractColumnNames.metering_point_id),
            F.col(ContractColumnNames.metering_point_type),
            F.col(ContractColumnNames.parent_metering_point_id),
            F.col(ContractColumnNames.period_from_date),
            F.col(ContractColumnNames.period_to_date),
            F.col(ContractColumnNames.settlement_month),
            F.col("execution_start_datetime"),
            F.col("cut_off_date"),
        )
    )

    return metering_points


def _split_periods_by_settlement_month(metering_points: DataFrame) -> DataFrame:
    """Create periods for settlement.

    This function creates periods for settlement by exploding the periods based on the settlement month.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - metering_point_type: Type of the metering point
            - parent_metering_point_id: ID of the parent metering point
            - period_from_date: Start date of the period
            - period_to_date: End date of the period
            - period_start: Start date of the period
            - period_end: End date of the period
            - cut_off_date: Cut off date
    """
    periods = (
        metering_points.select(
            "*",
            F.when(
                F.month(ContractColumnNames.period_from_date) >= F.col(ContractColumnNames.settlement_month),
                F.make_date(
                    F.year(F.col(ContractColumnNames.period_from_date)) + F.lit(1),
                    F.col(ContractColumnNames.settlement_month),
                    F.lit(1),
                ),
            )
            .otherwise(
                F.make_date(
                    F.year(F.col(ContractColumnNames.period_from_date)),
                    F.col(ContractColumnNames.settlement_month),
                    F.lit(1),
                )
            )
            .alias("next_settlement_date"),
        )
        .select(
            "*",
            F.posexplode(
                F.concat(
                    F.array(F.col(ContractColumnNames.period_from_date)),
                    F.sequence(
                        F.col("next_settlement_date"),
                        F.col(ContractColumnNames.period_to_date),
                        F.expr("INTERVAL 1 YEAR"),
                    ),
                    F.array(F.col(ContractColumnNames.period_to_date)),
                )
            ).alias("index", "period_start"),
        )
        .select(
            "*",
            F.lead("period_start", 1)
            .over(
                Window.partitionBy(
                    ContractColumnNames.metering_point_id,
                    ContractColumnNames.period_to_date,
                    ContractColumnNames.period_from_date,
                ).orderBy("index")
            )
            .alias("period_end"),
        )
        .where(F.datediff(F.col("period_end"), F.col("period_start")) > 1)
        .select(
            F.col(ContractColumnNames.metering_point_id),
            F.col(ContractColumnNames.metering_point_type),
            F.col(ContractColumnNames.parent_metering_point_id),
            F.col(ContractColumnNames.period_from_date),
            F.col(ContractColumnNames.period_to_date),
            F.col("period_start"),
            F.col("period_end"),
            F.col("cut_off_date"),
        )
    )
    return periods


def _determin_cut_off_for_periods(periods: DataFrame) -> DataFrame:
    """Apply cut off to periods.

    This function applies a cut off date to the periods data.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - metering_point_type: Type of the metering point
            - parent_metering_point_id: ID of the parent metering point
            - period_from_date: Start date of the period
            - period_to_date: End date of the period
            - period_start: Start date of the period with cut off
            - period_end: End date of the period
            - period_start_with_cut_off: Start date of the period with cut off
    """
    periods_w_cut_off = periods.select(
        "*",
        F.when(
            F.col("period_start") < F.col("cut_off_date"),
            F.col("cut_off_date"),
        )
        .otherwise(F.col("period_start"))
        .alias("period_start_with_cut_off"),
    ).select(
        F.col(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.metering_point_type),
        F.col(ContractColumnNames.parent_metering_point_id),
        F.col(ContractColumnNames.period_from_date),
        F.col(ContractColumnNames.period_to_date),
        F.col("period_start"),
        F.col("period_end"),
        F.col("period_start_with_cut_off"),
    )

    return periods_w_cut_off


def _join_metering_point_periods_to_time_series(
    filtered_time_series: DataFrame,
    periods: DataFrame,
) -> DataFrame:
    """Join time series to periods.

    This function joins the filtered time series data with the periods data.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - metering_point_type: Type of the metering point
            - parent_metering_point_id: ID of the parent metering point
            - period_start: Start date of the period
            - period_end: End date of the period
            - date: Observation time
            - quantity: The quantity as DecimalType(18, 3)
    """
    periods_with_ts = (
        periods.alias("mp")
        .join(
            filtered_time_series.alias("ts"),
            on=[
                F.col(f"mp.{ContractColumnNames.metering_point_id}")
                == F.col(f"ts.{ContractColumnNames.metering_point_id}"),
                (F.col(f"ts.{ContractColumnNames.date}") >= F.col("mp.period_start"))
                & (F.col(f"ts.{ContractColumnNames.date}") <= F.col("mp.period_end")),
            ],
            how="left",
        )
        .select(
            F.col(f"mp.{ContractColumnNames.metering_point_id}"),
            F.col(f"mp.{ContractColumnNames.metering_point_type}"),
            F.col(f"mp.{ContractColumnNames.parent_metering_point_id}"),
            F.col("mp.period_start"),
            F.col("mp.period_end"),
            F.col(f"ts.{ContractColumnNames.date}").alias(ContractColumnNames.date),
            F.col(f"ts.{ContractColumnNames.quantity}"),
        )
    )

    return periods_with_ts


def _sum_supply_and_consumption(periods_with_ts: DataFrame) -> DataFrame:
    """Aggregate consumption data over time series.

    This function aggregates consumption data over time series.

    Returns:
        DataFrame with columns:
            - parent_metering_point_id: ID of the parent metering point
            - period_start: Start date of the period
            - period_end: End date of the period
            - net_consumption: The calculated net consumption as DecimalType(18, 3)
    """
    net_consumption_over_ts = (
        periods_with_ts.groupBy(
            F.col(ContractColumnNames.parent_metering_point_id),
            F.col("period_start"),
            F.col("period_end"),
        )
        .agg(
            F.sum(
                F.when(
                    F.col(ContractColumnNames.metering_point_type) == MeteringPointType.CONSUMPTION_FROM_GRID.value,
                    F.col(ContractColumnNames.quantity),
                ).otherwise(0)
            ).alias(MeteringPointType.CONSUMPTION_FROM_GRID.value),
            F.sum(
                F.when(
                    F.col(ContractColumnNames.metering_point_type) == MeteringPointType.SUPPLY_TO_GRID.value,
                    F.col(ContractColumnNames.quantity),
                ).otherwise(0)
            ).alias(MeteringPointType.SUPPLY_TO_GRID.value),
        )
        .select(
            "*",
            (
                F.col(str(MeteringPointType.CONSUMPTION_FROM_GRID.value))
                - F.col(str(MeteringPointType.SUPPLY_TO_GRID.value))
            ).alias("net_consumption"),
        )
        .select(
            F.col(ContractColumnNames.parent_metering_point_id),
            F.col("period_start"),
            F.col("period_end"),
            F.col("net_consumption"),
        )
    )

    return net_consumption_over_ts


def _create_daily_quantity_per_period(
    periods_with_cut_off: DataFrame,
    net_consumption_over_ts: DataFrame,
) -> DataFrame:
    """Derive daily quantity from net consumption over time series.

    This function derives the daily quantity from net consumption over time series.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - period_start_with_cut_off: Start date of the period with cut off
            - period_end: End date of the period
            - daily_quantity: The calculated daily quantity as DecimalType(18, 3)
    """
    periods_with_net_consumption = (
        periods_with_cut_off.alias("mp")
        .join(
            net_consumption_over_ts.alias("ts"),
            on=[
                F.col(f"mp.{ContractColumnNames.parent_metering_point_id}")
                == F.col(f"ts.{ContractColumnNames.parent_metering_point_id}"),
                F.col("mp.period_start") == F.col("ts.period_start"),
                F.col("mp.period_end") == F.col("ts.period_end"),
                F.col(f"mp.{ContractColumnNames.metering_point_type}") == MeteringPointType.NET_CONSUMPTION.value,
            ],
            how="inner",
        )
        .select(
            "*",
            F.when(
                F.col("net_consumption") < 0,
                F.lit(0),
            )
            .otherwise(F.col("net_consumption") / F.datediff(F.col("mp.period_end"), F.col("mp.period_start")))
            .cast(T.DecimalType(18, 3))
            .alias("daily_quantity"),
        )
        .select(
            F.col(f"mp.{ContractColumnNames.metering_point_id}"),
            F.col("period_start_with_cut_off"),
            F.col("mp.period_end"),
            F.col("daily_quantity"),
        )
    )

    return periods_with_net_consumption
