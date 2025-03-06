from geh_common.domain.types import MeteringPointType, NetSettlementGroup
from geh_common.pyspark.transformations import days_in_year
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.electrical_heating.domain.ephemiral_column_names import EphemiralColumnNames

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
            (F.col(ContractColumnNames.net_settlement_group) == NetSettlementGroup.NET_SETTLEMENT_GROUP_2)
            & (F.col(EphemiralColumnNames.net_consumption_metering_point_id).isNotNull()),
            F.col(EphemiralColumnNames.net_consumption_metering_point_id),
        )
        .otherwise(F.col(ContractColumnNames.parent_metering_point_id))
        .alias(EphemiralColumnNames.energy_source_metering_point_id),
    )


def _join_source_metering_point_periods_with_energy_hourly(
    parent_and_child_metering_point_and_periods_in_localtime: DataFrame,
    time_series_points_hourly: DataFrame,
) -> DataFrame:
    consumption = time_series_points_hourly.where(
        F.col(ContractColumnNames.metering_point_type).isin(
            MeteringPointType.CONSUMPTION.value, MeteringPointType.NET_CONSUMPTION.value
        )
    )
    supply_to_grid = time_series_points_hourly.where(
        F.col(ContractColumnNames.metering_point_type) == F.lit(MeteringPointType.SUPPLY_TO_GRID.value)
    )
    consumption_from_grid = time_series_points_hourly.where(
        F.col(ContractColumnNames.metering_point_type) == F.lit(MeteringPointType.CONSUMPTION_FROM_GRID.value)
    )

    return (
        parent_and_child_metering_point_and_periods_in_localtime.alias("metering_point")
        # Join each (net)consumption metering point with the (net)consumption time series points
        # Inner join to only include metering points with energy data.
        .join(
            consumption.alias("consumption"),
            (
                F.col(f"consumption.{ContractColumnNames.metering_point_id}")
                == F.col(f"metering_point.{EphemiralColumnNames.energy_source_metering_point_id}")
            )
            & (
                F.col(f"consumption.{EphemiralColumnNames.observation_time_hourly_lt}")
                >= F.col(f"metering_point.{EphemiralColumnNames.overlap_period_start_lt}")
            )
            & (
                F.col(f"consumption.{EphemiralColumnNames.observation_time_hourly_lt}")
                < F.col(f"metering_point.{EphemiralColumnNames.overlap_period_end_lt}")
            )
            & (
                F.year(F.col(f"consumption.{EphemiralColumnNames.observation_time_hourly_lt}"))
                == F.year(F.col(f"metering_point.{EphemiralColumnNames.period_year_lt}"))
            ),
            "inner",
        )
        .join(
            supply_to_grid.alias("supply_to_grid"),
            (
                F.col(f"supply_to_grid.{ContractColumnNames.metering_point_id}")
                == F.col(f"metering_point.{EphemiralColumnNames.supply_to_grid_metering_point_id}")
            )
            & (
                F.col(f"supply_to_grid.{EphemiralColumnNames.observation_time_hourly}")
                == F.col(f"consumption.{EphemiralColumnNames.observation_time_hourly}")
            ),
            "left",
        )
        .join(
            consumption_from_grid.alias("consumption_from_grid"),
            (
                F.col(f"consumption_from_grid.{ContractColumnNames.metering_point_id}")
                == F.col(f"metering_point.{EphemiralColumnNames.consumption_from_grid_metering_point_id}")
            )
            & (
                F.col(f"consumption_from_grid.{EphemiralColumnNames.observation_time_hourly}")
                == F.col(f"consumption.{EphemiralColumnNames.observation_time_hourly}")
            ),
            "left",
        )
        .select(
            F.col(f"metering_point.{EphemiralColumnNames.parent_period_start}").alias(
                EphemiralColumnNames.parent_period_start
            ),
            F.col(f"metering_point.{EphemiralColumnNames.parent_period_end}").alias(
                EphemiralColumnNames.parent_period_end
            ),
            F.col(f"metering_point.{ContractColumnNames.net_settlement_group}").alias(
                ContractColumnNames.net_settlement_group
            ),
            F.col(f"metering_point.{EphemiralColumnNames.electrical_heating_metering_point_id}").alias(
                EphemiralColumnNames.electrical_heating_metering_point_id
            ),
            F.col(f"consumption.{EphemiralColumnNames.observation_time_hourly_lt}").alias(
                EphemiralColumnNames.observation_time_hourly_lt
            ),
            F.col(f"consumption.{ContractColumnNames.quantity}").alias(ContractColumnNames.quantity),
            F.col(f"supply_to_grid.{ContractColumnNames.metering_point_id}").alias(
                EphemiralColumnNames.supply_to_grid_metering_point_id
            ),
            F.col(f"supply_to_grid.{ContractColumnNames.quantity}").alias(EphemiralColumnNames.supply_to_grid_quantity),
            F.col(f"consumption_from_grid.{ContractColumnNames.metering_point_id}").alias(
                EphemiralColumnNames.consumption_from_grid_metering_point_id
            ),
            F.col(f"consumption_from_grid.{ContractColumnNames.quantity}").alias(
                EphemiralColumnNames.consumption_from_grid_quantity
            ),
        )
    )


def _calculate_period_limit(
    periods_with_energy_hourly: DataFrame,
) -> DataFrame:
    df = periods_with_energy_hourly.select(
        "*",
        F.coalesce(
            F.least(
                F.col(EphemiralColumnNames.consumption_from_grid_quantity),
                F.col(EphemiralColumnNames.supply_to_grid_quantity),
            ),
            F.lit(0),
        ).alias("nettoficated_hourly"),
    )

    period_window = Window.partitionBy(
        F.col(EphemiralColumnNames.electrical_heating_metering_point_id),
        F.col(EphemiralColumnNames.parent_period_start),
        F.col(EphemiralColumnNames.parent_period_end),
    )

    return (
        df.groupBy(
            EphemiralColumnNames.electrical_heating_metering_point_id,
            EphemiralColumnNames.parent_period_start,
            EphemiralColumnNames.parent_period_end,
            F.date_trunc("day", F.col(EphemiralColumnNames.observation_time_hourly_lt)).alias(ContractColumnNames.date),
        )
        .agg(
            F.sum(F.col(ContractColumnNames.quantity)).alias(ContractColumnNames.quantity),
            F.sum(F.col("nettoficated_hourly")).alias("nettoficated_daily"),
        )
        .select(
            "*",
            (
                F.datediff(
                    F.col(EphemiralColumnNames.parent_period_end), F.col(EphemiralColumnNames.parent_period_start)
                )
                * _ELECTRICAL_HEATING_LIMIT_YEARLY
                / days_in_year(F.col(EphemiralColumnNames.parent_period_start))
            ).alias("period_limit"),
            F.sum(F.col("nettoficated_daily")).over(period_window).alias("period_limit_reduction"),
        )
        .select(
            "*",
            (F.col("period_limit") - F.col("period_limit_reduction")).alias(EphemiralColumnNames.period_energy_limit),
        )
    )


def _aggregate_quantity_over_period(time_series_points: DataFrame) -> DataFrame:
    period_window = (
        Window.partitionBy(
            F.col(EphemiralColumnNames.electrical_heating_metering_point_id),
            F.col(EphemiralColumnNames.parent_period_start),
            F.col(EphemiralColumnNames.parent_period_end),
        )
        .orderBy(F.col(ContractColumnNames.date))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return time_series_points.select(
        F.sum(F.col(ContractColumnNames.quantity)).over(period_window).alias(EphemiralColumnNames.cumulative_quantity),
        F.col(EphemiralColumnNames.electrical_heating_metering_point_id),
        F.col(ContractColumnNames.date),
        F.col(ContractColumnNames.quantity),
        F.col(EphemiralColumnNames.period_energy_limit),
    ).drop_duplicates()


def _impose_period_quantity_limit(time_series_points: DataFrame) -> DataFrame:
    return time_series_points.select(
        F.when(
            (F.col(EphemiralColumnNames.cumulative_quantity) >= F.col(EphemiralColumnNames.period_energy_limit))
            & (
                F.col(EphemiralColumnNames.cumulative_quantity) - F.col(ContractColumnNames.quantity)
                < F.col(EphemiralColumnNames.period_energy_limit)
            ),
            F.col(EphemiralColumnNames.period_energy_limit)
            + F.col(ContractColumnNames.quantity)
            - F.col(EphemiralColumnNames.cumulative_quantity),
        )
        .when(
            F.col(EphemiralColumnNames.cumulative_quantity) > F.col(EphemiralColumnNames.period_energy_limit),
            0,
        )
        .otherwise(
            F.col(ContractColumnNames.quantity),
        )
        .cast(DecimalType(18, 3))
        .alias(ContractColumnNames.quantity),
        F.col(EphemiralColumnNames.cumulative_quantity),
        F.col(EphemiralColumnNames.electrical_heating_metering_point_id).alias(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.date),
        F.col(EphemiralColumnNames.period_energy_limit),
    ).drop_duplicates()
