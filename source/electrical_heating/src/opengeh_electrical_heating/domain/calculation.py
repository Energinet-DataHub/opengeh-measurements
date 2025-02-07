from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark_functions.functions import (
    convert_to_utc,
    days_in_year,
)
from telemetry_logging import use_span

import opengeh_electrical_heating.domain.transformations as t
from opengeh_electrical_heating.domain.calculated_measurements_daily import CalculatedMeasurementsDaily
from opengeh_electrical_heating.domain.calculated_names import CalculatedNames
from opengeh_electrical_heating.domain.column_names import ColumnNames
from opengeh_electrical_heating.domain.types.net_settlement_group import NetSettlementGroup

_ELECTRICAL_HEATING_LIMIT_YEARLY = 4000.0
"""Limit in kWh."""


# This is a temporary implementation. The final implementation will be provided in later PRs.
# This is also the function that will be tested using the `testcommon.etl` framework.
@use_span()
def execute(
    time_series_points: DataFrame,
    consumption_metering_point_periods: DataFrame,
    child_metering_points: DataFrame,
    time_zone: str,
) -> CalculatedMeasurementsDaily:
    old_consumption_energy = t.get_daily_consumption_energy_in_local_time(time_series_points, time_zone)

    old_electrical_heating = t.get_electrical_heating_in_local_time(time_series_points, time_zone)

    metering_point_periods = t.get_joined_metering_point_periods_in_local_time(
        consumption_metering_point_periods, child_metering_points, time_zone
    )

    metering_point_periods = _calculate_period_limit(metering_point_periods)
    metering_point_periods = _find_source_metering_point_for_energy(metering_point_periods)

    metering_point_periods_with_energy = _join_source_metering_point_periods_with_energy(
        old_consumption_energy,
        metering_point_periods,
    )
    metering_point_periods_with_energy = _filter_parent_child_overlap_period_and_year(
        metering_point_periods_with_energy
    )

    new_electrical_heating = _aggregate_quantity_over_period(metering_point_periods_with_energy)
    new_electrical_heating = _impose_period_quantity_limit(new_electrical_heating)
    new_electrical_heating = _filter_unchanged_electrical_heating(new_electrical_heating, old_electrical_heating)
    new_electrical_heating = convert_to_utc(new_electrical_heating, time_zone)

    return CalculatedMeasurementsDaily(new_electrical_heating)


def _calculate_period_limit(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        (
            F.datediff(F.col(CalculatedNames.parent_period_end), F.col(CalculatedNames.parent_period_start))
            * _ELECTRICAL_HEATING_LIMIT_YEARLY
            / days_in_year(F.col(CalculatedNames.parent_period_start))
        ).alias(CalculatedNames.period_energy_limit),
    )


def _find_source_metering_point_for_energy(metering_point_periods: DataFrame) -> DataFrame:
    """Determine which metering point to use for energy data.

    - For net settlement group 2: use the net consumption metering point
    - For other: use the consumption metering point (this will be updated when more net settlement groups are added)

    The metering point id is added as a column named `energy_source_metering_point_id`.
    """
    return metering_point_periods.select(
        "*",
        F.when(
            F.col(CalculatedNames.parent_net_settlement_group) == NetSettlementGroup.NET_SETTLEMENT_GROUP_2,
            F.col(CalculatedNames.net_consumption_metering_point_id),
        )
        .otherwise(F.col(ColumnNames.parent_metering_point_id))
        .alias(CalculatedNames.energy_source_metering_point_id),
    )


def _filter_unchanged_electrical_heating(
    newly_calculated_electrical_heating: DataFrame,
    electrical_heating_from_before: DataFrame,
) -> DataFrame:
    return (
        newly_calculated_electrical_heating.alias("current")
        .join(
            electrical_heating_from_before.alias("previous"),
            (
                (
                    F.col(f"current.{ColumnNames.metering_point_id}")
                    == F.col(f"previous.{ColumnNames.metering_point_id}")
                )
                & (F.col(f"current.{CalculatedNames.date}") == F.col(f"previous.{CalculatedNames.date}"))
                & (F.col(f"current.{ColumnNames.quantity}") == F.col(f"previous.{ColumnNames.quantity}"))
            ),
            "left_anti",
        )
        .select(
            F.col(f"current.{ColumnNames.metering_point_id}"),
            F.col(f"current.{CalculatedNames.date}"),
            F.col(f"current.{ColumnNames.quantity}"),
        )
    )


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
        .cast(T.DecimalType(18, 3))
        .alias(ColumnNames.quantity),
        F.col(CalculatedNames.cumulative_quantity),
        F.col(ColumnNames.metering_point_id),
        F.col(CalculatedNames.date),
        F.col(CalculatedNames.period_energy_limit),
    ).drop_duplicates()


def _aggregate_quantity_over_period(time_series_points: DataFrame) -> DataFrame:
    period_window = (
        Window.partitionBy(
            F.col(ColumnNames.metering_point_id),
            F.col(CalculatedNames.parent_period_start),
            F.col(CalculatedNames.parent_period_end),
        )
        .orderBy(F.col(CalculatedNames.date))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return time_series_points.select(
        F.sum(F.col(ColumnNames.quantity)).over(period_window).alias(CalculatedNames.cumulative_quantity),
        F.col(ColumnNames.metering_point_id),
        F.col(CalculatedNames.date),
        F.col(ColumnNames.quantity),
        F.col(CalculatedNames.period_energy_limit),
    ).drop_duplicates()


def _filter_parent_child_overlap_period_and_year(
    time_series_points: DataFrame,
) -> DataFrame:
    return time_series_points.where(
        (F.col(CalculatedNames.date) >= F.col(CalculatedNames.overlap_period_start))
        & (F.col(CalculatedNames.date) < F.col(CalculatedNames.overlap_period_end))
        & (F.year(F.col(CalculatedNames.date)) == F.year(F.col(CalculatedNames.period_year)))
    )


def _join_source_metering_point_periods_with_energy(
    time_series_points: DataFrame,
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return (
        time_series_points.alias("energy")
        .join(
            parent_and_child_metering_point_and_periods.alias("metering_point"),
            F.col(f"energy.{ColumnNames.metering_point_id}")
            == F.col(f"metering_point.{CalculatedNames.energy_source_metering_point_id}"),
            "inner",
        )
        .select(
            F.col(f"metering_point.{CalculatedNames.parent_period_start}").alias(CalculatedNames.parent_period_start),
            F.col(f"metering_point.{CalculatedNames.parent_period_end}").alias(CalculatedNames.parent_period_end),
            F.col(f"metering_point.{CalculatedNames.electrical_heating_metering_point_id}").alias(
                ColumnNames.metering_point_id
            ),
            F.col(f"energy.{CalculatedNames.date}").alias(CalculatedNames.date),
            F.col(f"energy.{ColumnNames.quantity}").alias(ColumnNames.quantity),
            F.col(f"metering_point.{CalculatedNames.period_energy_limit}").alias(CalculatedNames.period_energy_limit),
            F.col(f"metering_point.{CalculatedNames.overlap_period_start}").alias(CalculatedNames.overlap_period_start),
            F.col(f"metering_point.{CalculatedNames.overlap_period_end}").alias(CalculatedNames.overlap_period_end),
        )
    )
