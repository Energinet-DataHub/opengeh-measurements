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

from opengeh_calculated_measurements.opengeh_electrical_heating.domain.calculated_measurements_daily import CalculatedMeasurementsDaily
from opengeh_calculated_measurements.opengeh_electrical_heating.domain.column_names import ColumnNames
from opengeh_calculated_measurements.opengeh_electrical_heating.domain.types import NetSettlementGroup
from opengeh_calculated_measurements.opengeh_electrical_heating.domain.types.metering_point_type import MeteringPointType

_ELECTRICAL_HEATING_LIMIT_YEARLY = 4000.0
"""Limit in kWh."""


class _CalculatedNames:
    """Names of calculated columns."""

    cumulative_quantity = "cumulative_quantity"
    date = "date"
    electrical_heating_metering_point_id = "electrical_heating_metering_point_id"
    electrical_heating_period_end = "electrical_heating_period_end"
    electrical_heating_period_start = "electrical_heating_period_start"
    energy_source_metering_point_id = "energy_source_metering_point_id"
    """
    The metering point id from which to get the energy data.
    This is the net consumption metering point id if it exists, otherwise it's the consumption metering point id.
    """
    net_consumption_metering_point_id = "net_consumption_metering_point_id"
    net_consumption_period_end = "net_consumption_period_end"
    net_consumption_period_start = "net_consumption_period_start"
    overlap_period_end = "overlap_period_end"
    overlap_period_start = "overlap_period_start"
    parent_net_settlement_group = "parent_net_settlement_group"
    parent_period_end = "parent_period_end"
    parent_period_start = "parent_period_start"
    period_energy_limit = "period_energy_limit"
    period_year = "period_year"


# This is a temporary implementation. The final implementation will be provided in later PRs.
# This is also the function that will be tested using the `testcommon.etl` framework.
@use_span()
def execute(
    time_series_points: DataFrame,
    consumption_metering_point_periods: DataFrame,
    child_metering_points: DataFrame,
    time_zone: str,
) -> CalculatedMeasurementsDaily:
    energy = time_series_points.where(
        (F.col(ColumnNames.metering_point_type) == MeteringPointType.CONSUMPTION_METERING_POINT_TYPE.value)
        | (F.col(ColumnNames.metering_point_type) == MeteringPointType.NET_CONSUMPTION.value)
    )
    electrical_heating = time_series_points.where(
        F.col(ColumnNames.metering_point_type) == MeteringPointType.ELECTRICAL_HEATING.value
    )

    parent_metering_points = convert_from_utc(consumption_metering_point_periods, time_zone)
    child_metering_points = convert_from_utc(child_metering_points, time_zone)
    energy = convert_from_utc(energy, time_zone)
    electrical_heating = convert_from_utc(electrical_heating, time_zone)

    # prepare child metering points and parent metering points
    metering_point_periods = _join_children_to_parent_metering_point(child_metering_points, parent_metering_points)
    metering_point_periods = _close_open_ended_periods(metering_point_periods)
    metering_point_periods = _find_parent_child_overlap_period(metering_point_periods)
    metering_point_periods = _split_period_by_year(metering_point_periods)
    metering_point_periods = _calculate_period_limit(metering_point_periods)

    # prepare consumption and electrical heating time series data
    energy_daily = _calculate_daily_quantity(energy)
    previous_electrical_heating = _calculate_daily_quantity(electrical_heating)

    # determine from which metering point to get the consumption data (consumption or net consumption)
    metering_point_periods = _find_source_metering_point_for_energy(metering_point_periods)

    # here consumption time series and metering point periods data is joined
    metering_point_periods_with_energy = _join_source_metering_point_periods_with_energy(
        energy_daily,
        metering_point_periods,
    )
    energy = _filter_parent_child_overlap_period_and_year(metering_point_periods_with_energy)
    energy = _aggregate_quantity_over_period(energy)
    electrical_heating = _impose_period_quantity_limit(energy)
    electrical_heating = _filter_unchanged_electrical_heating(electrical_heating, previous_electrical_heating)

    electrical_heating = convert_to_utc(electrical_heating, time_zone)

    return CalculatedMeasurementsDaily(
        electrical_heating.orderBy(F.col(ColumnNames.metering_point_id), F.col(_CalculatedNames.date))
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
                & (F.col(f"current.{_CalculatedNames.date}") == F.col(f"previous.{_CalculatedNames.date}"))
                & (F.col(f"current.{ColumnNames.quantity}") == F.col(f"previous.{ColumnNames.quantity}"))
            ),
            "left_anti",
        )
        .select(
            F.col(f"current.{ColumnNames.metering_point_id}"),
            F.col(f"current.{_CalculatedNames.date}"),
            F.col(f"current.{ColumnNames.quantity}"),
        )
    )


def _impose_period_quantity_limit(time_series_points: DataFrame) -> DataFrame:
    return time_series_points.select(
        F.when(
            (F.col(_CalculatedNames.cumulative_quantity) >= F.col(_CalculatedNames.period_energy_limit))
            & (
                F.col(_CalculatedNames.cumulative_quantity) - F.col(ColumnNames.quantity)
                < F.col(_CalculatedNames.period_energy_limit)
            ),
            F.col(_CalculatedNames.period_energy_limit)
            + F.col(ColumnNames.quantity)
            - F.col(_CalculatedNames.cumulative_quantity),
        )
        .when(
            F.col(_CalculatedNames.cumulative_quantity) > F.col(_CalculatedNames.period_energy_limit),
            0,
        )
        .otherwise(
            F.col(ColumnNames.quantity),
        )
        .cast(T.DecimalType(18, 3))
        .alias(ColumnNames.quantity),
        F.col(_CalculatedNames.cumulative_quantity),
        F.col(ColumnNames.metering_point_id),
        F.col(_CalculatedNames.date),
        F.col(_CalculatedNames.period_energy_limit),
    ).drop_duplicates()


def _aggregate_quantity_over_period(time_series_points: DataFrame) -> DataFrame:
    period_window = (
        Window.partitionBy(
            F.col(ColumnNames.metering_point_id),
            F.col(_CalculatedNames.parent_period_start),
            F.col(_CalculatedNames.parent_period_end),
        )
        .orderBy(F.col(_CalculatedNames.date))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return time_series_points.select(
        F.sum(F.col(ColumnNames.quantity)).over(period_window).alias(_CalculatedNames.cumulative_quantity),
        F.col(ColumnNames.metering_point_id),
        F.col(_CalculatedNames.date),
        F.col(ColumnNames.quantity),
        F.col(_CalculatedNames.period_energy_limit),
    ).drop_duplicates()


def _filter_parent_child_overlap_period_and_year(
    time_series_points: DataFrame,
) -> DataFrame:
    return time_series_points.where(
        (F.col(_CalculatedNames.date) >= F.col(_CalculatedNames.overlap_period_start))
        & (F.col(_CalculatedNames.date) < F.col(_CalculatedNames.overlap_period_end))
        & (F.year(F.col(_CalculatedNames.date)) == F.year(F.col(_CalculatedNames.period_year)))
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
            == F.col(f"metering_point.{_CalculatedNames.energy_source_metering_point_id}"),
            "inner",
        )
        .select(
            F.col(f"metering_point.{_CalculatedNames.parent_period_start}").alias(_CalculatedNames.parent_period_start),
            F.col(f"metering_point.{_CalculatedNames.parent_period_end}").alias(_CalculatedNames.parent_period_end),
            F.col(f"metering_point.{_CalculatedNames.electrical_heating_metering_point_id}").alias(
                ColumnNames.metering_point_id
            ),
            F.col(f"energy.{_CalculatedNames.date}").alias(_CalculatedNames.date),
            F.col(f"energy.{ColumnNames.quantity}").alias(ColumnNames.quantity),
            F.col(f"metering_point.{_CalculatedNames.period_energy_limit}").alias(_CalculatedNames.period_energy_limit),
            F.col(f"metering_point.{_CalculatedNames.overlap_period_start}").alias(
                _CalculatedNames.overlap_period_start
            ),
            F.col(f"metering_point.{_CalculatedNames.overlap_period_end}").alias(_CalculatedNames.overlap_period_end),
        )
    )


def _calculate_daily_quantity(time_series: DataFrame) -> DataFrame:
    daily_window = Window.partitionBy(
        F.col(ColumnNames.metering_point_id),
        F.col(_CalculatedNames.date),
    )

    return (
        time_series.select(
            "*",
            F.date_trunc("day", F.col(ColumnNames.observation_time)).alias(_CalculatedNames.date),
        )
        .select(
            F.sum(F.col(ColumnNames.quantity)).over(daily_window).alias(ColumnNames.quantity),
            F.col(_CalculatedNames.date),
            F.col(ColumnNames.metering_point_id),
        )
        .drop_duplicates()
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
            F.col(_CalculatedNames.parent_net_settlement_group) == NetSettlementGroup.NET_SETTLEMENT_GROUP_2,
            F.col(_CalculatedNames.net_consumption_metering_point_id),
        )
        .otherwise(F.col(ColumnNames.parent_metering_point_id))
        .alias(_CalculatedNames.energy_source_metering_point_id),
    )


def _calculate_period_limit(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        (
            F.datediff(F.col(_CalculatedNames.parent_period_end), F.col(_CalculatedNames.parent_period_start))
            * _ELECTRICAL_HEATING_LIMIT_YEARLY
            / days_in_year(F.col(_CalculatedNames.parent_period_start))
        ).alias(_CalculatedNames.period_energy_limit),
    )


def _split_period_by_year(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        # create a row for each year in the period
        F.explode(
            F.sequence(
                begining_of_year(F.col(_CalculatedNames.parent_period_start)),
                F.coalesce(
                    begining_of_year(F.col(_CalculatedNames.parent_period_end)),
                    begining_of_year(F.current_date(), years_to_add=1),
                ),
                F.expr("INTERVAL 1 YEAR"),
            )
        ).alias(_CalculatedNames.period_year),
    ).select(
        F.col(ColumnNames.parent_metering_point_id),
        F.col(_CalculatedNames.parent_net_settlement_group),
        F.when(
            F.year(F.col(_CalculatedNames.parent_period_start)) == F.year(F.col(_CalculatedNames.period_year)),
            F.col(_CalculatedNames.parent_period_start),
        )
        .otherwise(begining_of_year(date=F.col(_CalculatedNames.period_year)))
        .alias(_CalculatedNames.parent_period_start),
        F.when(
            F.year(F.col(_CalculatedNames.parent_period_end)) == F.year(F.col(_CalculatedNames.period_year)),
            F.col(_CalculatedNames.parent_period_end),
        )
        .otherwise(begining_of_year(date=F.col(_CalculatedNames.period_year), years_to_add=1))
        .alias(_CalculatedNames.parent_period_end),
        F.col(_CalculatedNames.overlap_period_start),
        F.col(_CalculatedNames.overlap_period_end),
        F.col(_CalculatedNames.period_year),
        F.col(_CalculatedNames.electrical_heating_metering_point_id),
        F.col(_CalculatedNames.electrical_heating_period_start),
        F.col(_CalculatedNames.electrical_heating_period_end),
        F.col(_CalculatedNames.net_consumption_metering_point_id),
        F.col(_CalculatedNames.net_consumption_period_start),
        F.col(_CalculatedNames.net_consumption_period_end),
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
            F.col(_CalculatedNames.parent_period_start),
            F.col(_CalculatedNames.electrical_heating_period_start),
            F.col(_CalculatedNames.net_consumption_period_start),
        ).alias(_CalculatedNames.overlap_period_start),
        F.least(
            F.coalesce(
                F.col(_CalculatedNames.parent_period_end),
                begining_of_year(F.current_date(), years_to_add=1),
            ),
            F.coalesce(
                F.col(_CalculatedNames.electrical_heating_period_end),
                begining_of_year(F.current_date(), years_to_add=1),
            ),
            F.coalesce(
                F.col(_CalculatedNames.net_consumption_period_end),
                begining_of_year(F.current_date(), years_to_add=1),
            ),
        ).alias(_CalculatedNames.overlap_period_end),
    ).where(F.col(_CalculatedNames.overlap_period_start) < F.col(_CalculatedNames.overlap_period_end))


def _close_open_ended_periods(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    """Close open ended periods by setting the end date to the end of the current year."""
    return parent_and_child_metering_point_and_periods.select(
        # Consumption metering point
        F.col(ColumnNames.parent_metering_point_id),
        F.col(_CalculatedNames.parent_net_settlement_group),
        F.col(_CalculatedNames.parent_period_start),
        F.coalesce(
            F.col(_CalculatedNames.parent_period_end),
            begining_of_year(F.current_date(), years_to_add=1),
        ).alias(_CalculatedNames.parent_period_end),
        # Electrical heating metering point
        F.col(_CalculatedNames.electrical_heating_period_start),
        F.coalesce(
            F.col(_CalculatedNames.electrical_heating_period_end),
            begining_of_year(F.current_date(), years_to_add=1),
        ).alias(_CalculatedNames.electrical_heating_period_end),
        F.col(_CalculatedNames.electrical_heating_metering_point_id),
        # Net consumption metering point
        F.col(_CalculatedNames.net_consumption_period_start),
        F.coalesce(
            F.col(_CalculatedNames.net_consumption_period_end),
            begining_of_year(F.current_date(), years_to_add=1),
        ).alias(_CalculatedNames.net_consumption_period_end),
        F.col(_CalculatedNames.net_consumption_metering_point_id),
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
            F.col(f"parent.{ColumnNames.net_settlement_group}").alias(_CalculatedNames.parent_net_settlement_group),
            F.col(f"parent.{ColumnNames.period_from_date}").alias(_CalculatedNames.parent_period_start),
            F.col(f"parent.{ColumnNames.period_to_date}").alias(_CalculatedNames.parent_period_end),
            F.col(f"electrical_heating.{ColumnNames.metering_point_id}").alias(
                _CalculatedNames.electrical_heating_metering_point_id
            ),
            F.col(f"electrical_heating.{ColumnNames.coupled_date}").alias(
                _CalculatedNames.electrical_heating_period_start
            ),
            F.col(f"electrical_heating.{ColumnNames.uncoupled_date}").alias(
                _CalculatedNames.electrical_heating_period_end
            ),
            F.col(f"net_consumption.{ColumnNames.metering_point_id}").alias(
                _CalculatedNames.net_consumption_metering_point_id
            ),
            F.col(f"net_consumption.{ColumnNames.coupled_date}").alias(_CalculatedNames.net_consumption_period_start),
            F.col(f"net_consumption.{ColumnNames.uncoupled_date}").alias(_CalculatedNames.net_consumption_period_end),
        )
    )
