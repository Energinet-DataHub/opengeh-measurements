from datetime import datetime

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
def execute_logic(
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    current_measurements: CurrentMeasurements,
    time_zone: str,
    execution_start_datetime: datetime,
) -> DataFrame:
    # Filter and join the data
    filtered_time_series = current_measurements.df.where(
        F.col(ContractColumnNames.metering_point_type).isin(
            MeteringPointType.SUPPLY_TO_GRID.value, MeteringPointType.CONSUMPTION_FROM_GRID.value
        )
    )

    parent_child_joined = _join_child_to_consumption(
        consumption_metering_point_periods, child_metering_points, execution_start_datetime
    )
    filtered_time_series = convert_from_utc(filtered_time_series, time_zone)
    parent_child_joined = convert_from_utc(parent_child_joined, time_zone)

    # enforce 3 year
    metering_points = _generate_metering_points_with_cut_off(parent_child_joined)

    # split periods by settelement month
    periods = _create_periods_for_settlement(metering_points)

    # filter out periods that are not in the last 3 years
    periods_with_cut_off = _apply_cut_off_to_periods(periods)

    # ts for each period
    periods_with_ts = _join_time_series_to_periods(filtered_time_series, periods)

    # sum ts over each period
    net_consumption_over_ts = _aggregate_consumption_data(periods_with_ts)

    # calculate daily quantity
    periods_with_net_consumption = _derive_daily_quantity_from_net(periods_with_cut_off, net_consumption_over_ts)

    # generate daily observations quantity
    cnc_measurements = _expand_periods_to_daily_observations(periods_with_net_consumption)

    # check if any cnc differs from the newly calculated
    cnc_diff = _get_cnc_discrepancies(periods_with_ts, cnc_measurements)

    cnc_diff_utc = convert_to_utc(cnc_diff, time_zone)

    return cnc_diff_utc


def _get_cnc_discrepancies(periods_with_ts, cnc_measurements) -> DataFrame:
    """Get discrepancies between the calculated and the original CNC measurements.

    This function checks for discrepancies between the calculated CNC measurements and the original CNC measurements.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - observation_time: Daily observation time
            - quantity: The calculated daily quantity as DecimalType(18, 3)
    """
    cnc_diff = (
        cnc_measurements.alias("cnc")
        .join(
            periods_with_ts.alias("ts"),
            on=[
                F.col(f"cnc.{ContractColumnNames.metering_point_id}")
                == F.col(f"ts.{ContractColumnNames.metering_point_id}"),
                F.col(f"cnc.{ContractColumnNames.date}") == F.col(f"ts.{ContractColumnNames.observation_time}"),
            ],
            how="left_anti",
        )
        .select(
            F.col(ContractColumnNames.metering_point_id).alias(ContractColumnNames.metering_point_id),
            F.col(f"cnc.{ContractColumnNames.date}").alias(ContractColumnNames.date),
            F.col("cnc.daily_quantity").alias(ContractColumnNames.quantity),
        )
    )

    return cnc_diff


def _expand_periods_to_daily_observations(periods_with_net_consumption) -> DataFrame:
    """Expand periods with net consumption to daily observations.

    This function expands the periods with net consumption into daily observations.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - observation_time: Daily observation time
            - daily_quantity: The calculated daily quantity as DecimalType(18, 3)
    """
    cnc_measurements = periods_with_net_consumption.select(
        "*",
        F.explode(
            F.sequence(
                F.col("period_start_with_cut_off"),
                F.date_add(ContractColumnNames.period_to_date, -1),
                F.expr("INTERVAL 1 DAY"),
            )
        ).alias(ContractColumnNames.date),
    ).select(
        F.col(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.date),
        F.col("daily_quantity").cast(T.DecimalType(18, 3)),
    )

    return cnc_measurements


def _derive_daily_quantity_from_net(periods_with_cut_off, net_consumption_over_ts) -> DataFrame:
    """Derive daily quantity from net consumption over time series.

    This function derives the daily quantity from net consumption over time series.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - period_start_with_cut_off: Start date of the period with cut off
            - period_to_date: End date of the period
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
            .otherwise(
                F.col("net_consumption")
                / F.datediff(F.col(ContractColumnNames.period_to_date), F.col(ContractColumnNames.period_from_date))
            )
            .cast(T.DecimalType(18, 3))
            .alias("daily_quantity"),
        )
        .select(
            F.col(f"mp.{ContractColumnNames.metering_point_id}"),
            F.col(ContractColumnNames.period_to_date),
            F.col("period_start_with_cut_off"),
            F.col("daily_quantity"),
        )
    )

    return periods_with_net_consumption


def _aggregate_consumption_data(periods_with_ts) -> DataFrame:
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


def _join_time_series_to_periods(filtered_time_series, periods) -> DataFrame:
    """Join time series to periods.

    This function joins the filtered time series data with the periods data.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - metering_point_type: Type of the metering point
            - parent_metering_point_id: ID of the parent metering point
            - period_start: Start date of the period
            - period_end: End date of the period
            - observation_time: Observation time
            - quantity: The quantity as DecimalType(18, 3)
    """
    periods_with_ts = (
        periods.alias("mp")
        .join(
            filtered_time_series.alias("ts"),
            on=[
                F.col(f"mp.{ContractColumnNames.metering_point_id}")
                == F.col(f"ts.{ContractColumnNames.metering_point_id}"),
                (F.col(f"ts.{ContractColumnNames.observation_time}") >= F.col("mp.period_start"))
                & (F.col(f"ts.{ContractColumnNames.observation_time}") <= F.col("mp.period_end")),
            ],
            how="left",
        )
        .select(
            F.col(f"mp.{ContractColumnNames.metering_point_id}"),
            F.col(f"mp.{ContractColumnNames.metering_point_type}"),
            F.col(f"mp.{ContractColumnNames.parent_metering_point_id}"),
            F.col("mp.period_start"),
            F.col("mp.period_end"),
            F.col(f"ts.{ContractColumnNames.observation_time}"),
            F.col(f"ts.{ContractColumnNames.quantity}"),
        )
    )

    return periods_with_ts


def _apply_cut_off_to_periods(periods) -> DataFrame:
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
    periods_with_cut_off = periods.select(
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

    return periods_with_cut_off


def _create_periods_for_settlement(metering_points) -> DataFrame:
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


def _generate_metering_points_with_cut_off(parent_child_joined) -> DataFrame:
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


def _join_child_to_consumption(
    consumption_metering_point_periods, child_metering_points, execution_start_datetime
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
