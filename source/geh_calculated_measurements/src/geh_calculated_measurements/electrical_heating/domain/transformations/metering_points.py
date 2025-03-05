from geh_common.domain.types import MeteringPointType, NetSettlementGroup
from geh_common.pyspark.transformations import (
    begining_of_year,
    convert_from_utc,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    EphemiralColumnNames,
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
                F.col(ContractColumnNames.metering_point_type) == MeteringPointType.ELECTRICAL_HEATING.value
            ).alias("electrical_heating"),
            F.col(f"electrical_heating.{ContractColumnNames.parent_metering_point_id}")
            == F.col(f"parent.{ContractColumnNames.metering_point_id}"),
            "inner",
        )
        # Left join because there is - and need - not always be a net consumption metering point
        .join(
            child_metering_point_and_periods.where(
                F.col(ContractColumnNames.metering_point_type) == MeteringPointType.NET_CONSUMPTION.value
            ).alias("net_consumption"),
            (
                F.col(f"net_consumption.{ContractColumnNames.parent_metering_point_id}")
                == F.col(f"parent.{ContractColumnNames.metering_point_id}")
            )
            & (
                F.col(f"parent.{ContractColumnNames.net_settlement_group}").isin(
                    NetSettlementGroup.NET_SETTLEMENT_GROUP_2, NetSettlementGroup.NET_SETTLEMENT_GROUP_6
                )
            ),
            "left",
        )
        # Left join because there is - and need - not always be a net consumption from grid metering point
        .join(
            child_metering_point_and_periods.where(
                F.col(ContractColumnNames.metering_point_type) == MeteringPointType.CONSUMPTION_FROM_GRID.value
            ).alias("consumption_from_grid"),
            (
                F.col(f"consumption_from_grid.{ContractColumnNames.parent_metering_point_id}")
                == F.col(f"parent.{ContractColumnNames.metering_point_id}")
            )
            & (
                F.col(f"parent.{ContractColumnNames.net_settlement_group}").isin(
                    NetSettlementGroup.NET_SETTLEMENT_GROUP_2, NetSettlementGroup.NET_SETTLEMENT_GROUP_6
                )
            ),
            "left",
        )
        # Left join because there is - and need - not always be a net supply to grid metering point
        .join(
            child_metering_point_and_periods.where(
                F.col(ContractColumnNames.metering_point_type) == MeteringPointType.SUPPLY_TO_GRID.value
            ).alias("supply_to_grid"),
            (
                F.col(f"supply_to_grid.{ContractColumnNames.parent_metering_point_id}")
                == F.col(f"parent.{ContractColumnNames.metering_point_id}")
            )
            & (
                F.col(f"parent.{ContractColumnNames.net_settlement_group}").isin(
                    NetSettlementGroup.NET_SETTLEMENT_GROUP_2, NetSettlementGroup.NET_SETTLEMENT_GROUP_6
                )
            ),
            "left",
        )
        .select(
            F.col(f"parent.{ContractColumnNames.metering_point_id}").alias(
                ContractColumnNames.parent_metering_point_id
            ),
            F.col(f"parent.{ContractColumnNames.net_settlement_group}").alias(ContractColumnNames.net_settlement_group),
            F.col(f"parent.{ContractColumnNames.period_from_date}").alias(EphemiralColumnNames.parent_period_start),
            F.col(f"parent.{ContractColumnNames.period_to_date}").alias(EphemiralColumnNames.parent_period_end),
            F.col(f"electrical_heating.{ContractColumnNames.metering_point_id}").alias(
                EphemiralColumnNames.electrical_heating_metering_point_id
            ),
            F.col(f"electrical_heating.{ContractColumnNames.coupled_date}").alias(
                EphemiralColumnNames.electrical_heating_period_start
            ),
            F.col(f"electrical_heating.{ContractColumnNames.uncoupled_date}").alias(
                EphemiralColumnNames.electrical_heating_period_end
            ),
            F.col(f"net_consumption.{ContractColumnNames.metering_point_id}").alias(
                EphemiralColumnNames.net_consumption_metering_point_id
            ),
            F.col(f"net_consumption.{ContractColumnNames.coupled_date}").alias(
                EphemiralColumnNames.net_consumption_period_start
            ),
            F.col(f"net_consumption.{ContractColumnNames.uncoupled_date}").alias(
                EphemiralColumnNames.net_consumption_period_end
            ),
            F.col(f"consumption_from_grid.{ContractColumnNames.metering_point_id}").alias(
                EphemiralColumnNames.consumption_from_grid_metering_point_id
            ),
            F.col(f"consumption_from_grid.{ContractColumnNames.coupled_date}").alias(
                EphemiralColumnNames.consumption_from_grid_period_start
            ),
            F.col(f"consumption_from_grid.{ContractColumnNames.uncoupled_date}").alias(
                EphemiralColumnNames.consumption_from_grid_period_end
            ),
            F.col(f"supply_to_grid.{ContractColumnNames.metering_point_id}").alias(
                EphemiralColumnNames.supply_to_grid_metering_point_id
            ),
            F.col(f"supply_to_grid.{ContractColumnNames.coupled_date}").alias(
                EphemiralColumnNames.supply_to_grid_period_start
            ),
            F.col(f"supply_to_grid.{ContractColumnNames.uncoupled_date}").alias(
                EphemiralColumnNames.supply_to_grid_period_end
            ),
        )
    )


def _close_open_ended_periods(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    """Close open ended periods by setting the end date to the end of the current year."""
    return parent_and_child_metering_point_and_periods.select(
        "*", begining_of_year(F.current_date(), years_to_add=1).alias("end_of_year")
    ).select(
        # Consumption metering point
        F.col(ContractColumnNames.parent_metering_point_id),
        F.col(ContractColumnNames.net_settlement_group),
        F.col(EphemiralColumnNames.parent_period_start),
        F.coalesce(
            F.col(EphemiralColumnNames.parent_period_end),
            "end_of_year",
        ).alias(EphemiralColumnNames.parent_period_end),
        # Electrical heating metering point
        F.col(EphemiralColumnNames.electrical_heating_period_start),
        F.coalesce(
            F.col(EphemiralColumnNames.electrical_heating_period_end),
            "end_of_year",
        ).alias(EphemiralColumnNames.electrical_heating_period_end),
        F.col(EphemiralColumnNames.electrical_heating_metering_point_id),
        # Net consumption metering point
        F.col(EphemiralColumnNames.net_consumption_period_start),
        F.coalesce(
            F.col(EphemiralColumnNames.net_consumption_period_end),
            "end_of_year",
        ).alias(EphemiralColumnNames.net_consumption_period_end),
        F.col(EphemiralColumnNames.net_consumption_metering_point_id),
        # Consumption from grid metering point
        EphemiralColumnNames.consumption_from_grid_metering_point_id,
        EphemiralColumnNames.consumption_from_grid_period_start,
        F.coalesce(EphemiralColumnNames.consumption_from_grid_period_end, "end_of_year").alias(
            EphemiralColumnNames.consumption_from_grid_period_end
        ),
        # Supply to grid metering point
        EphemiralColumnNames.supply_to_grid_metering_point_id,
        EphemiralColumnNames.supply_to_grid_period_start,
        F.coalesce(EphemiralColumnNames.supply_to_grid_period_end, "end_of_year").alias(
            EphemiralColumnNames.supply_to_grid_period_end
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
            F.col(EphemiralColumnNames.parent_period_start),
            F.col(EphemiralColumnNames.electrical_heating_period_start),
            F.col(EphemiralColumnNames.net_consumption_period_start),
            F.col(EphemiralColumnNames.consumption_from_grid_period_start),
            F.col(EphemiralColumnNames.supply_to_grid_period_start),
        ).alias(EphemiralColumnNames.overlap_period_start_lt),
        F.least(
            F.col(EphemiralColumnNames.parent_period_end),
            F.col(EphemiralColumnNames.electrical_heating_period_end),
            F.col(EphemiralColumnNames.net_consumption_period_end),
            F.col(EphemiralColumnNames.consumption_from_grid_period_end),
            F.col(EphemiralColumnNames.supply_to_grid_period_end),
        ).alias(EphemiralColumnNames.overlap_period_end_lt),
    ).where(F.col(EphemiralColumnNames.overlap_period_start_lt) < F.col(EphemiralColumnNames.overlap_period_end_lt))


def _split_period_by_year(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        # create a row for each year in the period
        F.explode(
            F.sequence(
                begining_of_year(F.col(EphemiralColumnNames.parent_period_start)),
                F.coalesce(
                    # Subtract a tiny bit to avoid including the next year if the period ends at new year
                    begining_of_year(F.expr(f"{EphemiralColumnNames.parent_period_end} - INTERVAL 1 SECOND")),
                    begining_of_year(F.current_date(), years_to_add=1),
                ),
                F.expr("INTERVAL 1 YEAR"),
            )
        ).alias("period_year"),
    ).select(
        F.col(ContractColumnNames.parent_metering_point_id),
        F.col(ContractColumnNames.net_settlement_group),
        F.when(
            F.year(F.col(EphemiralColumnNames.parent_period_start)) == F.year(F.col("period_year")),
            F.col(EphemiralColumnNames.parent_period_start),
        )
        .otherwise(begining_of_year(date=F.col("period_year")))
        .alias(EphemiralColumnNames.parent_period_start),
        F.when(
            F.year(F.col(EphemiralColumnNames.parent_period_end)) == F.year(F.col("period_year")),
            F.col(EphemiralColumnNames.parent_period_end),
        )
        .otherwise(begining_of_year(date=F.col("period_year"), years_to_add=1))
        .alias(EphemiralColumnNames.parent_period_end),
        F.col(EphemiralColumnNames.overlap_period_start_lt),
        F.col(EphemiralColumnNames.overlap_period_end_lt),
        F.col(EphemiralColumnNames.period_year_lt),
        F.col(EphemiralColumnNames.electrical_heating_metering_point_id),
        F.col(EphemiralColumnNames.net_consumption_metering_point_id),
        F.col(EphemiralColumnNames.consumption_from_grid_metering_point_id),
        F.col(EphemiralColumnNames.supply_to_grid_metering_point_id),
    )


def _remove_nsg2_up2end_without_netcomsumption(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.where(
        ~(
            (F.col(ContractColumnNames.net_settlement_group) == NetSettlementGroup.NET_SETTLEMENT_GROUP_2)
            & (F.col(EphemiralColumnNames.net_consumption_metering_point_id).isNull())
            & (F.year(F.col(EphemiralColumnNames.period_year_lt)) == F.year(F.current_date()))
        )
    )
