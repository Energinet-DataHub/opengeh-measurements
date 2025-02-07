from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark_functions.functions import (
    begining_of_year,
    convert_from_utc,
    convert_to_utc,
    days_in_year,
)
from telemetry_logging import use_span

import opengeh_electrical_heating.domain.transformations as t
from opengeh_electrical_heating.domain.calculated_measurements_daily import CalculatedMeasurementsDaily
from opengeh_electrical_heating.domain.calculated_names import CalculatedNames
from opengeh_electrical_heating.domain.column_names import ColumnNames
from opengeh_electrical_heating.domain.types import NetSettlementGroup
from opengeh_electrical_heating.domain.types.metering_point_type import MeteringPointType

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

    old_electrical_heating = time_series_points.where(
        F.col(ColumnNames.metering_point_type) == MeteringPointType.ELECTRICAL_HEATING.value
    )
    old_electrical_heating = convert_from_utc(old_electrical_heating, time_zone)
    old_electrical_heating = t.calculate_daily_quantity(old_electrical_heating)

    metering_point_periods = _join_children_to_parent_metering_point(
        child_metering_points, consumption_metering_point_periods
    )
    metering_point_periods = convert_from_utc(metering_point_periods, time_zone)
    metering_point_periods = _close_open_ended_periods(metering_point_periods)
    metering_point_periods = _find_parent_child_overlap_period(metering_point_periods)
    metering_point_periods = _split_period_by_year(metering_point_periods)
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


def _split_period_by_year(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        # create a row for each year in the period
        F.explode(
            F.sequence(
                begining_of_year(F.col(CalculatedNames.parent_period_start)),
                F.coalesce(
                    begining_of_year(F.col(CalculatedNames.parent_period_end)),
                    begining_of_year(F.current_date(), years_to_add=1),
                ),
                F.expr("INTERVAL 1 YEAR"),
            )
        ).alias(CalculatedNames.period_year),
    ).select(
        F.col(ColumnNames.parent_metering_point_id),
        F.col(CalculatedNames.parent_net_settlement_group),
        F.when(
            F.year(F.col(CalculatedNames.parent_period_start)) == F.year(F.col(CalculatedNames.period_year)),
            F.col(CalculatedNames.parent_period_start),
        )
        .otherwise(begining_of_year(date=F.col(CalculatedNames.period_year)))
        .alias(CalculatedNames.parent_period_start),
        F.when(
            F.year(F.col(CalculatedNames.parent_period_end)) == F.year(F.col(CalculatedNames.period_year)),
            F.col(CalculatedNames.parent_period_end),
        )
        .otherwise(begining_of_year(date=F.col(CalculatedNames.period_year), years_to_add=1))
        .alias(CalculatedNames.parent_period_end),
        F.col(CalculatedNames.overlap_period_start),
        F.col(CalculatedNames.overlap_period_end),
        F.col(CalculatedNames.period_year),
        F.col(CalculatedNames.electrical_heating_metering_point_id),
        F.col(CalculatedNames.electrical_heating_period_start),
        F.col(CalculatedNames.electrical_heating_period_end),
        F.col(CalculatedNames.net_consumption_metering_point_id),
        F.col(CalculatedNames.net_consumption_period_start),
        F.col(CalculatedNames.net_consumption_period_end),
    )


def _find_parent_child_overlap_period(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        # Here we calculate the overlapping period between the consumption metering point period
        # and the children metering point periods.
        # We, however, assume that there is only one overlapping period between the periods
        F.greatest(
            F.col(CalculatedNames.parent_period_start),
            F.col(CalculatedNames.electrical_heating_period_start),
            F.col(CalculatedNames.net_consumption_period_start),
        ).alias(CalculatedNames.overlap_period_start),
        F.least(
            F.coalesce(
                F.col(CalculatedNames.parent_period_end),
                begining_of_year(F.current_date(), years_to_add=1),
            ),
            F.coalesce(
                F.col(CalculatedNames.electrical_heating_period_end),
                begining_of_year(F.current_date(), years_to_add=1),
            ),
            F.coalesce(
                F.col(CalculatedNames.net_consumption_period_end),
                begining_of_year(F.current_date(), years_to_add=1),
            ),
        ).alias(CalculatedNames.overlap_period_end),
    ).where(F.col(CalculatedNames.overlap_period_start) < F.col(CalculatedNames.overlap_period_end))


def _close_open_ended_periods(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    """Close open ended periods by setting the end date to the end of the current year."""
    return parent_and_child_metering_point_and_periods.select(
        # Consumption metering point
        F.col(ColumnNames.parent_metering_point_id),
        F.col(CalculatedNames.parent_net_settlement_group),
        F.col(CalculatedNames.parent_period_start),
        F.coalesce(
            F.col(CalculatedNames.parent_period_end),
            begining_of_year(F.current_date(), years_to_add=1),
        ).alias(CalculatedNames.parent_period_end),
        # Electrical heating metering point
        F.col(CalculatedNames.electrical_heating_period_start),
        F.coalesce(
            F.col(CalculatedNames.electrical_heating_period_end),
            begining_of_year(F.current_date(), years_to_add=1),
        ).alias(CalculatedNames.electrical_heating_period_end),
        F.col(CalculatedNames.electrical_heating_metering_point_id),
        # Net consumption metering point
        F.col(CalculatedNames.net_consumption_period_start),
        F.coalesce(
            F.col(CalculatedNames.net_consumption_period_end),
            begining_of_year(F.current_date(), years_to_add=1),
        ).alias(CalculatedNames.net_consumption_period_end),
        F.col(CalculatedNames.net_consumption_metering_point_id),
    )


def _join_children_to_parent_metering_point(
    child_metering_point_and_periods: DataFrame,
    parent_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return (
        parent_metering_point_and_periods.alias("parent")
        # Inner join because there is no reason to calculate if there is no electrical heating metering point
        .join(
            child_metering_point_and_periods.where(
                F.col(ColumnNames.metering_point_type) == MeteringPointType.ELECTRICAL_HEATING.value
            ).alias("electrical_heating"),
            F.col(f"electrical_heating.{ColumnNames.parent_metering_point_id}")
            == F.col(f"parent.{ColumnNames.metering_point_id}"),
            "inner",
        )
        # Left join because there is - and need - not always be a net consumption metering point
        # Net consumption is only relevant for net settlement group 2
        .join(
            child_metering_point_and_periods.where(
                F.col(ColumnNames.metering_point_type) == MeteringPointType.NET_CONSUMPTION.value
            ).alias("net_consumption"),
            F.col(f"net_consumption.{ColumnNames.parent_metering_point_id}")
            == F.col(f"parent.{ColumnNames.metering_point_id}"),
            "left",
        )
        .select(
            F.col(f"parent.{ColumnNames.metering_point_id}").alias(ColumnNames.parent_metering_point_id),
            F.col(f"parent.{ColumnNames.net_settlement_group}").alias(CalculatedNames.parent_net_settlement_group),
            F.col(f"parent.{ColumnNames.period_from_date}").alias(CalculatedNames.parent_period_start),
            F.col(f"parent.{ColumnNames.period_to_date}").alias(CalculatedNames.parent_period_end),
            F.col(f"electrical_heating.{ColumnNames.metering_point_id}").alias(
                CalculatedNames.electrical_heating_metering_point_id
            ),
            F.col(f"electrical_heating.{ColumnNames.coupled_date}").alias(
                CalculatedNames.electrical_heating_period_start
            ),
            F.col(f"electrical_heating.{ColumnNames.uncoupled_date}").alias(
                CalculatedNames.electrical_heating_period_end
            ),
            F.col(f"net_consumption.{ColumnNames.metering_point_id}").alias(
                CalculatedNames.net_consumption_metering_point_id
            ),
            F.col(f"net_consumption.{ColumnNames.coupled_date}").alias(CalculatedNames.net_consumption_period_start),
            F.col(f"net_consumption.{ColumnNames.uncoupled_date}").alias(CalculatedNames.net_consumption_period_end),
        )
    )
