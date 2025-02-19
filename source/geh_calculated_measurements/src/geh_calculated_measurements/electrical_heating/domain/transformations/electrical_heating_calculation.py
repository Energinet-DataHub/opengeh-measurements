from geh_common.domain.types import MeteringPointType, NetSettlementGroup
from geh_common.pyspark.transformations import days_in_year
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from geh_calculated_measurements.electrical_heating.domain.calculated_names import CalculatedNames
from geh_calculated_measurements.electrical_heating.domain.column_names import ColumnNames
from geh_calculated_measurements.electrical_heating.domain.transformations.time_series_points import (
    get_hourly_energy_in_local_time,
)

_ELECTRICAL_HEATING_LIMIT_YEARLY = 4000.0
"""Limit in kWh."""


def calculate_electrical_heating_in_local_time(
    time_series_points_in_utc: DataFrame, periods_in_localtime: DataFrame, time_zone: str
) -> DataFrame:
    periods_in_localtime = _find_source_metering_point_for_energy(periods_in_localtime)

    periods_with_hourly_energy = _join_source_metering_point_periods_with_energy(
        periods_in_localtime,
        time_series_points_in_utc,
        time_zone,
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
            F.col(ColumnNames.net_settlement_group) == NetSettlementGroup.NET_SETTLEMENT_GROUP_2,
            F.col(CalculatedNames.net_consumption_metering_point_id),
        )
        .otherwise(F.col(ColumnNames.parent_metering_point_id))
        .alias(CalculatedNames.energy_source_metering_point_id),
    )


def _join_source_metering_point_periods_with_energy(
    parent_and_child_metering_point_and_periods_in_localtime: DataFrame,
    time_series_points_in_utc: DataFrame,
    time_zone: str,
) -> DataFrame:
    # TODO: What if there are data on both metering points?
    consumption = get_hourly_energy_in_local_time(
        time_series_points_in_utc, time_zone, [MeteringPointType.CONSUMPTION, MeteringPointType.NET_CONSUMPTION]
    )
    supply_to_grid = get_hourly_energy_in_local_time(
        time_series_points_in_utc, time_zone, [MeteringPointType.SUPPLY_TO_GRID]
    )
    consumption_from_grid = get_hourly_energy_in_local_time(
        time_series_points_in_utc, time_zone, [MeteringPointType.CONSUMPTION_FROM_GRID]
    )

    df = (
        parent_and_child_metering_point_and_periods_in_localtime.alias("metering_point")
        # Join each (net)consumption metering point with the (net)consumption time series points
        # Inner join to only include metering points with energy data.
        # TODO DOC: Only include time series points within the overlapping period of the (net)consumption metering point.
        .join(
            consumption.alias("consumption"),
            (
                F.col(f"consumption.{ColumnNames.metering_point_id}")
                == F.col(f"metering_point.{CalculatedNames.energy_source_metering_point_id}")
            )
            & (
                F.col(f"consumption.{CalculatedNames.observation_time_hourly}")
                >= F.col(f"metering_point.{CalculatedNames.overlap_period_start}")
            )
            & (
                F.col(f"consumption.{CalculatedNames.observation_time_hourly}")
                < F.col(f"metering_point.{CalculatedNames.overlap_period_end}")
            )
            & (
                F.year(F.col(f"consumption.{CalculatedNames.observation_time_hourly}"))
                == F.year(F.col(CalculatedNames.period_year))
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
                F.col(f"supply_to_grid.{CalculatedNames.observation_time_hourly}")
                == F.col(f"consumption.{CalculatedNames.observation_time_hourly}")
            ),
            "left",
        )
        .select(
            F.col(f"metering_point.{CalculatedNames.parent_period_start}").alias(CalculatedNames.parent_period_start),
            F.col(f"metering_point.{CalculatedNames.parent_period_end}").alias(CalculatedNames.parent_period_end),
            F.col(f"metering_point.{ColumnNames.net_settlement_group}").alias(ColumnNames.net_settlement_group),
            F.col(f"metering_point.{CalculatedNames.electrical_heating_metering_point_id}").alias(
                CalculatedNames.electrical_heating_metering_point_id
            ),
            F.col(f"metering_point.{CalculatedNames.overlap_period_start}").alias(CalculatedNames.overlap_period_start),
            F.col(f"metering_point.{CalculatedNames.overlap_period_end}").alias(CalculatedNames.overlap_period_end),
            F.col(f"consumption.{CalculatedNames.observation_time_hourly}").alias(
                CalculatedNames.observation_time_hourly
            ),
            F.col(f"consumption.{ColumnNames.quantity}").alias(CalculatedNames.consumption_quantity),
            F.col(f"supply_to_grid.{ColumnNames.quantity}").alias(CalculatedNames.supply_to_grid_quantity),
            F.col(f"consumption_from_grid.{ColumnNames.quantity}").alias(
                CalculatedNames.consumption_from_grid_quantity
            ),
            # TODO BJM: Remove
            F.col(f"consumption.{ColumnNames.quantity}").alias(ColumnNames.quantity),
        )
    )
    return df


def _calculate_period_limit(
    periods_with_energy: DataFrame,
) -> DataFrame:
    df = periods_with_energy.select(
        "*",
        (
            F.datediff(F.col(CalculatedNames.parent_period_end), F.col(CalculatedNames.parent_period_start))
            * _ELECTRICAL_HEATING_LIMIT_YEARLY
            / days_in_year(F.col(CalculatedNames.parent_period_start))
        ).alias(CalculatedNames.period_energy_limit),
    )

    # TODO: Calculate reduction

    # Aggregate the energy data to daily
    df = df.groupBy(
        CalculatedNames.electrical_heating_metering_point_id,
        # TODO: Why not the overlap period?
        CalculatedNames.parent_period_start,
        CalculatedNames.parent_period_end,
        F.date_trunc("day", F.col(CalculatedNames.observation_time_hourly)).alias(ColumnNames.date),
        # ColumnNames.quantity,
        CalculatedNames.period_energy_limit,
    ).agg(F.sum(F.col(ColumnNames.quantity)).alias(ColumnNames.quantity))

    return df


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
