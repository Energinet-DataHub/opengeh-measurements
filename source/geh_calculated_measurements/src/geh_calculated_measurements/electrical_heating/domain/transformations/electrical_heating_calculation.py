from geh_common.domain.types import MeteringPointType, NetSettlementGroup
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from geh_calculated_measurements.common.domain import ColumnNames
from geh_calculated_measurements.electrical_heating.domain.calculated_names import CalculatedNames
from geh_calculated_measurements.electrical_heating.domain.debug import debugging

_ELECTRICAL_HEATING_LIMIT_YEARLY = 4000.0
"""Limit in kWh."""


def calculate_electrical_heating_in_local_time(time_series_points_hourly: DataFrame, periods: DataFrame) -> DataFrame:
    periods = _find_source_metering_point_for_energy(periods)

    periods_with_hourly_energy = _join_source_metering_point_periods_with_energy_hourly(
        periods,
        time_series_points_hourly,
    )
    periods_with_daily_energy_and_limit = _calculate_period_limit(periods_with_hourly_energy)

    new_electrical_heating = _aggregate_quantity_over_period(periods_with_daily_energy_and_limit)
    new_electrical_heating = _impose_period_quantity_limit(new_electrical_heating)
    return new_electrical_heating


def _find_source_metering_point_for_energy(metering_point_periods: DataFrame) -> DataFrame:
    """Determine which metering point to use for energy data.

    - For net settlement group 2: use the net consumption metering point
    - For other: use the consumption metering point (this will be updated when more net settlement groups are added)

    The metering point id is added as a column named `energy_source_metering_point_id`.
    """
    return metering_point_periods.select(
        "*",
        F.when(
            (
                F.col(ColumnNames.net_settlement_group).isin(
                    NetSettlementGroup.NET_SETTLEMENT_GROUP_2, NetSettlementGroup.NET_SETTLEMENT_GROUP_6
                )
            )
            & (F.col(CalculatedNames.net_consumption_metering_point_id).isNotNull()),
            F.col(CalculatedNames.net_consumption_metering_point_id),
        )
        .otherwise(F.col(ColumnNames.parent_metering_point_id))
        .alias(CalculatedNames.energy_source_metering_point_id),
    )


@debugging()
def _join_source_metering_point_periods_with_energy_hourly(
    parent_and_child_metering_point_and_periods_in_localtime: DataFrame,
    time_series_points_hourly: DataFrame,
) -> DataFrame:
    consumption = time_series_points_hourly.where(
        F.col(ColumnNames.metering_point_type).isin(
            MeteringPointType.CONSUMPTION.value, MeteringPointType.NET_CONSUMPTION.value
        )
    )
    supply_to_grid = time_series_points_hourly.where(
        F.col(ColumnNames.metering_point_type) == F.lit(MeteringPointType.SUPPLY_TO_GRID.value)
    )
    consumption_from_grid = time_series_points_hourly.where(
        F.col(ColumnNames.metering_point_type) == F.lit(MeteringPointType.CONSUMPTION_FROM_GRID.value)
    )

    return (
        parent_and_child_metering_point_and_periods_in_localtime.alias("metering_point")
        # Join each (net)consumption metering point with the (net)consumption time series points
        # Inner join to only include metering points with energy data.
        .join(
            consumption.alias("consumption"),
            (
                F.col(f"consumption.{ColumnNames.metering_point_id}")
                == F.col(f"metering_point.{CalculatedNames.energy_source_metering_point_id}")
            )
            & (
                F.col(f"consumption.{CalculatedNames.observation_time_hourly_lt}")
                >= F.col(f"metering_point.{CalculatedNames.overlap_period_start_lt}")
            )
            & (
                F.col(f"consumption.{CalculatedNames.observation_time_hourly_lt}")
                < F.col(f"metering_point.{CalculatedNames.overlap_period_end_lt}")
            )
            & (
                (
                    F.col(f"consumption.{CalculatedNames.observation_time_hourly}")
                    >= F.col(f"metering_point.{CalculatedNames.settlement_month_datetime}")
                )
                & (
                    F.col(f"consumption.{CalculatedNames.observation_time_hourly}")
                    < F.add_months(F.col(f"metering_point.{CalculatedNames.settlement_month_datetime}"), 12)
                )
            ),
            "inner",
        )
        .join(
            supply_to_grid.alias("supply_to_grid"),
            (
                F.col(f"supply_to_grid.{ColumnNames.metering_point_id}")
                == F.col(f"metering_point.{CalculatedNames.supply_to_grid_metering_point_id}")
            )
            & (
                F.col(f"supply_to_grid.{CalculatedNames.observation_time_hourly}")
                == F.col(f"consumption.{CalculatedNames.observation_time_hourly}")
            ),
            "left",
        )
        .join(
            consumption_from_grid.alias("consumption_from_grid"),
            (
                F.col(f"consumption_from_grid.{ColumnNames.metering_point_id}")
                == F.col(f"metering_point.{CalculatedNames.consumption_from_grid_metering_point_id}")
            )
            & (
                F.col(f"consumption_from_grid.{CalculatedNames.observation_time_hourly}")
                == F.col(f"consumption.{CalculatedNames.observation_time_hourly}")
            ),
            "left",
        )
        .select(
            F.col(f"metering_point.{CalculatedNames.parent_period_start}").alias(CalculatedNames.parent_period_start),
            F.col(f"metering_point.{CalculatedNames.parent_period_end}").alias(CalculatedNames.parent_period_end),
            F.col(f"metering_point.{ColumnNames.net_settlement_group}").alias(ColumnNames.net_settlement_group),
            F.col(f"metering_point.{CalculatedNames.settlement_month_datetime}").alias(
                CalculatedNames.settlement_month_datetime
            ),
            F.col(f"metering_point.{CalculatedNames.electrical_heating_metering_point_id}").alias(
                CalculatedNames.electrical_heating_metering_point_id
            ),
            F.col(f"consumption.{CalculatedNames.observation_time_hourly_lt}").alias(
                CalculatedNames.observation_time_hourly_lt
            ),
            F.col(f"consumption.{ColumnNames.quantity}").alias(ColumnNames.quantity),
            F.col(f"supply_to_grid.{ColumnNames.metering_point_id}").alias(
                CalculatedNames.supply_to_grid_metering_point_id
            ),
            F.col(f"supply_to_grid.{ColumnNames.quantity}").alias(CalculatedNames.supply_to_grid_quantity),
            F.col(f"consumption_from_grid.{ColumnNames.metering_point_id}").alias(
                CalculatedNames.consumption_from_grid_metering_point_id
            ),
            F.col(f"consumption_from_grid.{ColumnNames.quantity}").alias(
                CalculatedNames.consumption_from_grid_quantity
            ),
        )
    )


@debugging()
def _calculate_period_limit(
    periods_with_energy_hourly: DataFrame,
) -> DataFrame:
    df = periods_with_energy_hourly.select(
        "*",
        F.coalesce(
            F.least(
                F.col(CalculatedNames.consumption_from_grid_quantity), F.col(CalculatedNames.supply_to_grid_quantity)
            ),
            F.lit(0),
        ).alias("nettoficated_hourly"),
    )

    period_window = Window.partitionBy(
        F.col(CalculatedNames.electrical_heating_metering_point_id),
        F.col(CalculatedNames.parent_period_start),
        F.col(CalculatedNames.parent_period_end),
    )

    return (
        df.groupBy(
            CalculatedNames.electrical_heating_metering_point_id,
            CalculatedNames.parent_period_start,
            CalculatedNames.parent_period_end,
            F.date_trunc("day", F.col(CalculatedNames.observation_time_hourly_lt)).alias(ColumnNames.date),
        )
        .agg(
            F.sum(F.col(ColumnNames.quantity)).alias(ColumnNames.quantity),
            F.sum(F.col("nettoficated_hourly")).alias("nettoficated_daily"),
            F.first(CalculatedNames.settlement_month_datetime).alias(CalculatedNames.settlement_month_datetime),
        )
        .select(
            "*",
            (
                F.datediff(F.col(CalculatedNames.parent_period_end), F.col(CalculatedNames.parent_period_start))
                * _ELECTRICAL_HEATING_LIMIT_YEARLY
                / _days_in_settlement_year(F.col(CalculatedNames.settlement_month_datetime))
            ).alias("period_limit"),
            F.sum(F.col("nettoficated_daily")).over(period_window).alias("period_limit_reduction"),
        )
        .select(
            "*",
            (F.col("period_limit") - F.col("period_limit_reduction")).alias(CalculatedNames.period_energy_limit),
        )
    )


def _days_in_settlement_year(settlement_month_datetime: Column) -> Column:
    return F.datediff(F.add_months(settlement_month_datetime, 12), settlement_month_datetime)


def _aggregate_quantity_over_period(time_series_points: DataFrame) -> DataFrame:
    period_window = (
        Window.partitionBy(
            F.col(CalculatedNames.electrical_heating_metering_point_id),
            F.col(CalculatedNames.parent_period_start),
            F.col(CalculatedNames.parent_period_end),
        )
        .orderBy(F.col(ColumnNames.date))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return time_series_points.select(
        F.sum(F.col(ColumnNames.quantity)).over(period_window).alias(CalculatedNames.cumulative_quantity),
        F.col(CalculatedNames.electrical_heating_metering_point_id),
        F.col(ColumnNames.date),
        F.col(ColumnNames.quantity),
        F.col(CalculatedNames.period_energy_limit),
    ).drop_duplicates()


def _impose_period_quantity_limit(time_series_points: DataFrame) -> DataFrame:
    return time_series_points.select(
        F.when(
            (F.col(CalculatedNames.cumulative_quantity) >= F.col(CalculatedNames.period_energy_limit))
            & (
                F.col(CalculatedNames.cumulative_quantity) - F.col(ColumnNames.quantity)
                < F.col(CalculatedNames.period_energy_limit)
            ),
            F.col(CalculatedNames.period_energy_limit)
            + F.col(ColumnNames.quantity)
            - F.col(CalculatedNames.cumulative_quantity),
        )
        .when(
            F.col(CalculatedNames.cumulative_quantity) > F.col(CalculatedNames.period_energy_limit),
            0,
        )
        .otherwise(
            F.col(ColumnNames.quantity),
        )
        .cast(DecimalType(18, 3))
        .alias(ColumnNames.quantity),
        F.col(CalculatedNames.cumulative_quantity),
        F.col(CalculatedNames.electrical_heating_metering_point_id).alias(ColumnNames.metering_point_id),
        F.col(ColumnNames.date),
        F.col(CalculatedNames.period_energy_limit),
    ).drop_duplicates()
