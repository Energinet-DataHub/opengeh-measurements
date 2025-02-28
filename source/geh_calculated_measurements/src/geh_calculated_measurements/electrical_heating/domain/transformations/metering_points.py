from geh_common.domain.types import MeteringPointType, NetSettlementGroup
from geh_common.pyspark.transformations import (
    begining_of_year,
    convert_from_utc,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ColumnNames
from geh_calculated_measurements.electrical_heating.domain import (
    CalculatedNames,
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
    metering_point_periods = _split_period_by_settlement_month(metering_point_periods)
    metering_point_periods = _remove_nsg2_up2end_without_netcomsumption(metering_point_periods)

    return metering_point_periods


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
        .join(
            child_metering_point_and_periods.where(
                F.col(ColumnNames.metering_point_type) == MeteringPointType.NET_CONSUMPTION.value
            ).alias("net_consumption"),
            (
                F.col(f"net_consumption.{ColumnNames.parent_metering_point_id}")
                == F.col(f"parent.{ColumnNames.metering_point_id}")
            )
            & (
                F.col(f"parent.{ColumnNames.net_settlement_group}").isin(
                    NetSettlementGroup.NET_SETTLEMENT_GROUP_2, NetSettlementGroup.NET_SETTLEMENT_GROUP_6
                )
            ),
            "left",
        )
        # Left join because there is - and need - not always be a net consumption from grid metering point
        .join(
            child_metering_point_and_periods.where(
                F.col(ColumnNames.metering_point_type) == MeteringPointType.CONSUMPTION_FROM_GRID.value
            ).alias("consumption_from_grid"),
            (
                F.col(f"consumption_from_grid.{ColumnNames.parent_metering_point_id}")
                == F.col(f"parent.{ColumnNames.metering_point_id}")
            )
            & (
                F.col(f"parent.{ColumnNames.net_settlement_group}").isin(
                    NetSettlementGroup.NET_SETTLEMENT_GROUP_2, NetSettlementGroup.NET_SETTLEMENT_GROUP_6
                )
            ),
            "left",
        )
        # Left join because there is - and need - not always be a net supply to grid metering point
        .join(
            child_metering_point_and_periods.where(
                F.col(ColumnNames.metering_point_type) == MeteringPointType.SUPPLY_TO_GRID.value
            ).alias("supply_to_grid"),
            (
                F.col(f"supply_to_grid.{ColumnNames.parent_metering_point_id}")
                == F.col(f"parent.{ColumnNames.metering_point_id}")
            )
            & (
                F.col(f"parent.{ColumnNames.net_settlement_group}").isin(
                    NetSettlementGroup.NET_SETTLEMENT_GROUP_2, NetSettlementGroup.NET_SETTLEMENT_GROUP_6
                )
            ),
            "left",
        )
        .select(
            F.col(f"parent.{ColumnNames.metering_point_id}").alias(ColumnNames.parent_metering_point_id),
            F.col(f"parent.{ColumnNames.net_settlement_group}").alias(ColumnNames.net_settlement_group),
            F.col(f"parent.{ColumnNames.settlement_month}").alias(ColumnNames.settlement_month),
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
    """Close open ended periods by setting the end date to the end of the current settlement year."""
    # TODO BJM: Next step HERE
    return parent_and_child_metering_point_and_periods.select(
        "*", begining_of_year(F.current_date(), years_to_add=1).alias("end_of_year")
    ).select(
        # Consumption metering point
        F.col(ColumnNames.parent_metering_point_id),
        F.col(ColumnNames.net_settlement_group),
        F.col(ColumnNames.settlement_month),
        F.col(CalculatedNames.parent_period_start),
        F.coalesce(
            F.col(CalculatedNames.parent_period_end),
            "end_of_year",
        ).alias(CalculatedNames.parent_period_end),
        # Electrical heating metering point
        F.col(CalculatedNames.electrical_heating_period_start),
        F.coalesce(
            F.col(CalculatedNames.electrical_heating_period_end),
            "end_of_year",
        ).alias(CalculatedNames.electrical_heating_period_end),
        F.col(CalculatedNames.electrical_heating_metering_point_id),
        # Net consumption metering point
        F.col(CalculatedNames.net_consumption_period_start),
        F.coalesce(
            F.col(CalculatedNames.net_consumption_period_end),
            "end_of_year",
        ).alias(CalculatedNames.net_consumption_period_end),
        F.col(CalculatedNames.net_consumption_metering_point_id),
        # Consumption from grid metering point
        CalculatedNames.consumption_from_grid_metering_point_id,
        CalculatedNames.consumption_from_grid_period_start,
        F.coalesce(CalculatedNames.consumption_from_grid_period_end, "end_of_year").alias(
            CalculatedNames.consumption_from_grid_period_end
        ),
        # Supply to grid metering point
        CalculatedNames.supply_to_grid_metering_point_id,
        CalculatedNames.supply_to_grid_period_start,
        F.coalesce(CalculatedNames.supply_to_grid_period_end, "end_of_year").alias(
            CalculatedNames.supply_to_grid_period_end
        ),
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
            F.col(CalculatedNames.consumption_from_grid_period_start),
            F.col(CalculatedNames.supply_to_grid_period_start),
        ).alias(CalculatedNames.overlap_period_start),
        F.least(
            F.col(CalculatedNames.parent_period_end),
            F.col(CalculatedNames.electrical_heating_period_end),
            F.col(CalculatedNames.net_consumption_period_end),
            F.col(CalculatedNames.consumption_from_grid_period_end),
            F.col(CalculatedNames.supply_to_grid_period_end),
        ).alias(CalculatedNames.overlap_period_end),
    ).where(F.col(CalculatedNames.overlap_period_start) < F.col(CalculatedNames.overlap_period_end))


def _split_period_by_settlement_month(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        # create a row for each year in the period
        F.explode(
            F.sequence(
                begining_of_year(F.col(CalculatedNames.parent_period_start)),
                F.coalesce(
                    # Subtract a tiny bit to avoid including the next year if the period ends at new year
                    begining_of_year(F.expr(f"{CalculatedNames.parent_period_end} - INTERVAL 1 SECOND")),
                    begining_of_year(F.current_date(), years_to_add=1),
                ),
                F.expr("INTERVAL 1 YEAR"),
            )
        ).alias("period_year"),
    ).select(
        F.col(ColumnNames.parent_metering_point_id),
        F.col(ColumnNames.net_settlement_group),
        F.when(
            F.year(F.col(CalculatedNames.parent_period_start)) == F.year(F.col("period_year")),
            F.col(CalculatedNames.parent_period_start),
        )
        .otherwise(begining_of_year(date=F.col("period_year")))
        .alias(CalculatedNames.parent_period_start),
        F.when(
            F.year(F.col(CalculatedNames.parent_period_end)) == F.year(F.col("period_year")),
            F.col(CalculatedNames.parent_period_end),
        )
        .otherwise(begining_of_year(date=F.col("period_year"), years_to_add=1))
        .alias(CalculatedNames.parent_period_end),
        F.col(CalculatedNames.overlap_period_start),
        F.col(CalculatedNames.overlap_period_end),
        F.col("period_year").alias(CalculatedNames.settlement_month_datetime),
        F.col(CalculatedNames.electrical_heating_metering_point_id),
        F.col(CalculatedNames.net_consumption_metering_point_id),
        F.col(CalculatedNames.consumption_from_grid_metering_point_id),
        F.col(CalculatedNames.supply_to_grid_metering_point_id),
    )


def _remove_nsg2_up2end_without_netcomsumption(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.where(
        ~(
            (F.col(ColumnNames.net_settlement_group) == NetSettlementGroup.NET_SETTLEMENT_GROUP_2)
            & (F.col(CalculatedNames.net_consumption_metering_point_id).isNull())
            & (F.year(F.col(CalculatedNames.settlement_month_datetime)) == F.year(F.current_date()))
        )
    )
