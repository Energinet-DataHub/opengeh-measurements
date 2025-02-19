from geh_common.domain.types import MeteringPointType
from geh_common.pyspark.transformations import (
    begining_of_year,
    convert_from_utc,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.electrical_heating.domain.calculated_names import (
    CalculatedNames,
)
from geh_calculated_measurements.electrical_heating.domain.column_names import (
    ColumnNames,
)
from geh_calculated_measurements.electrical_heating.infrastructure import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)


def get_joined_metering_point_periods_in_local_time(
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_zone: str,
) -> DataFrame:
    metering_point_periods = _join_children_to_parent_metering_point(
        child_metering_points.df, consumption_metering_point_periods.df
    )
    metering_point_periods = convert_from_utc(metering_point_periods, time_zone)
    metering_point_periods = _close_open_ended_periods(metering_point_periods)
    metering_point_periods = _find_parent_child_overlap_period(metering_point_periods)
    metering_point_periods = _split_period_by_year(metering_point_periods)
    return metering_point_periods


def _join_children_to_parent_metering_point(
    child_metering_point_and_periods: DataFrame,
    parent_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return (
        # TODO: What if E17 has different - say D14 - child metering points at different times?
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
        .join(
            child_metering_point_and_periods.where(
                F.col(ColumnNames.metering_point_type) == MeteringPointType.CONSUMPTION_FROM_GRID.value
            ).alias("consumption_from_grid"),
            F.col(f"consumption_from_grid.{ColumnNames.parent_metering_point_id}")
            == F.col(f"parent.{ColumnNames.metering_point_id}"),
            "left",
        )
        .join(
            child_metering_point_and_periods.where(
                F.col(ColumnNames.metering_point_type) == MeteringPointType.SUPPLY_TO_GRID.value
            ).alias("supply_to_grid"),
            F.col(f"supply_to_grid.{ColumnNames.parent_metering_point_id}")
            == F.col(f"parent.{ColumnNames.metering_point_id}"),
            "left",
        )
        .select(
            F.col(f"parent.{ColumnNames.metering_point_id}").alias(ColumnNames.parent_metering_point_id),
            F.col(f"parent.{ColumnNames.net_settlement_group}").alias(ColumnNames.net_settlement_group),
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
            F.col(f"consumption_from_grid.{ColumnNames.metering_point_id}").alias(
                CalculatedNames.consumption_from_grid_metering_point_id
            ),
            F.col(f"consumption_from_grid.{ColumnNames.coupled_date}").alias(
                CalculatedNames.consumption_from_grid_period_start
            ),
            F.col(f"consumption_from_grid.{ColumnNames.uncoupled_date}").alias(
                CalculatedNames.consumption_from_grid_period_end
            ),
            F.col(f"supply_to_grid.{ColumnNames.metering_point_id}").alias(
                CalculatedNames.supply_to_grid_metering_point_id
            ),
            F.col(f"supply_to_grid.{ColumnNames.coupled_date}").alias(CalculatedNames.supply_to_grid_period_start),
            F.col(f"supply_to_grid.{ColumnNames.uncoupled_date}").alias(CalculatedNames.supply_to_grid_period_end),
        )
    )


def _close_open_ended_periods(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    """Close open ended periods by setting the end date to the end of the current year."""
    # TODO: Do we need this for other than E17?
    return parent_and_child_metering_point_and_periods.select(
        # Consumption metering point
        F.col(ColumnNames.parent_metering_point_id),
        F.col(ColumnNames.net_settlement_group),
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
        CalculatedNames.consumption_from_grid_metering_point_id,
        CalculatedNames.consumption_from_grid_period_start,
        CalculatedNames.consumption_from_grid_period_end,
        CalculatedNames.supply_to_grid_metering_point_id,
        CalculatedNames.supply_to_grid_period_start,
        CalculatedNames.supply_to_grid_period_end,
    )


def _find_parent_child_overlap_period(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        # Here we calculate the overlapping period between the consumption metering point period
        # and the children metering point periods.
        # We, however, assume that there is only one overlapping period between the periods
        # TODO: Should we add D06 and D07 here?
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


def _split_period_by_year(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    # TODO: Why do we need to select all the periods?
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
        F.col(ColumnNames.net_settlement_group),
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
        F.col(CalculatedNames.consumption_from_grid_metering_point_id),
        F.col(CalculatedNames.consumption_from_grid_period_start),
        F.col(CalculatedNames.consumption_from_grid_period_end),
        F.col(CalculatedNames.supply_to_grid_metering_point_id),
        F.col(CalculatedNames.supply_to_grid_period_start),
        F.col(CalculatedNames.supply_to_grid_period_end),
    )
