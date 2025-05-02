from datetime import datetime
from zoneinfo import ZoneInfo

from geh_common.domain.types import MeteringPointType, NetSettlementGroup
from geh_common.pyspark.transformations import convert_from_utc
from geh_common.testing.dataframes import testing
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    EphemeralColumnNames,
)


@testing()
def get_joined_metering_point_periods_in_local_time(
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_zone: str,
    execution_start_datetime: datetime,
) -> DataFrame:
    execution_start_datetime_local_time = execution_start_datetime.astimezone(ZoneInfo(time_zone))

    consumption_metering_point_periods = add_settlement_year_end_datetime(
        consumption_metering_point_periods.df,
        execution_start_datetime_local_time,
    )

    metering_point_periods = _join_children_to_parent_metering_point(
        child_metering_points.df,
        consumption_metering_point_periods,
    )

    metering_point_periods = _close_open_ended_periods(
        metering_point_periods,
        execution_start_datetime_local_time,
    )
    metering_point_periods = convert_from_utc(
        metering_point_periods,
        time_zone,
    )

    metering_point_periods = _find_parent_child_overlap_period(metering_point_periods)
    metering_point_periods = _split_period_by_settlement_year(
        metering_point_periods, execution_start_datetime_local_time
    )
    metering_point_periods = _remove_net_settlement_group_2_up2end_without_netconsumption(
        metering_point_periods, execution_start_datetime_local_time
    )

    return metering_point_periods


def add_settlement_year_end_datetime(
    metering_point_periods: DataFrame,
    execution_start_datetime_local_time: datetime,
) -> DataFrame:
    """Add the settlement year end datetime to the metering point periods."""
    return metering_point_periods.select(
        "*",
        _settlement_year_end_datetime(
            F.col(ContractColumnNames.settlement_month), execution_start_datetime_local_time
        ).alias("settlement_year_end"),
    ).select(
        F.col("metering_point_id"),
        F.col("has_electrical_heating"),
        F.col("net_settlement_group"),
        F.col("settlement_month"),
        F.col("period_from_date"),
        F.col("settlement_year_end"),
        F.coalesce(
            F.col(ContractColumnNames.period_to_date),
            F.col("settlement_year_end"),
        ).alias(ContractColumnNames.period_to_date),
    )


@testing()
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
            (
                F.col(f"electrical_heating.{ContractColumnNames.parent_metering_point_id}")
                == F.col(f"parent.{ContractColumnNames.metering_point_id}")
            )
            & has_overlapping_period(
                F.col(f"parent.{ContractColumnNames.period_from_date}"),
                F.col(f"parent.{ContractColumnNames.period_to_date}"),
                F.col(f"electrical_heating.{ContractColumnNames.coupled_date}"),
                F.coalesce(
                    F.col(f"electrical_heating.{ContractColumnNames.uncoupled_date}"),
                    F.col("parent.settlement_year_end"),
                ),
            ),
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
            )
            & has_overlapping_period(
                F.col(f"parent.{ContractColumnNames.period_from_date}"),
                F.col(f"parent.{ContractColumnNames.period_to_date}"),
                F.col(f"net_consumption.{ContractColumnNames.coupled_date}"),
                F.coalesce(
                    F.col(f"net_consumption.{ContractColumnNames.uncoupled_date}"),
                    F.col("settlement_year_end"),
                ),
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
            )
            & has_overlapping_period(
                F.col(f"parent.{ContractColumnNames.period_from_date}"),
                F.col(f"parent.{ContractColumnNames.period_to_date}"),
                F.col(f"consumption_from_grid.{ContractColumnNames.coupled_date}"),
                F.coalesce(
                    F.col(f"consumption_from_grid.{ContractColumnNames.uncoupled_date}"),
                    F.col("settlement_year_end"),
                ),
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
            )
            & has_overlapping_period(
                F.col(f"parent.{ContractColumnNames.period_from_date}"),
                F.col(f"parent.{ContractColumnNames.period_to_date}"),
                F.col(f"supply_to_grid.{ContractColumnNames.coupled_date}"),
                F.coalesce(
                    F.col(f"supply_to_grid.{ContractColumnNames.uncoupled_date}"),
                    F.col("settlement_year_end"),
                ),
            ),
            "left",
        )
        .select(
            F.col(f"parent.{ContractColumnNames.metering_point_id}").alias(
                ContractColumnNames.parent_metering_point_id
            ),
            F.col(f"parent.{ContractColumnNames.net_settlement_group}").alias(ContractColumnNames.net_settlement_group),
            F.col(f"parent.{ContractColumnNames.settlement_month}").alias(ContractColumnNames.settlement_month),
            F.col(f"parent.{ContractColumnNames.period_from_date}").alias(EphemeralColumnNames.parent_period_start),
            F.col(f"parent.{ContractColumnNames.period_to_date}").alias(EphemeralColumnNames.parent_period_end),
            F.col(f"electrical_heating.{ContractColumnNames.metering_point_id}").alias(
                EphemeralColumnNames.electrical_heating_metering_point_id
            ),
            F.col(f"electrical_heating.{ContractColumnNames.coupled_date}").alias(
                EphemeralColumnNames.electrical_heating_period_start
            ),
            F.col(f"electrical_heating.{ContractColumnNames.uncoupled_date}").alias(
                EphemeralColumnNames.electrical_heating_period_end
            ),
            F.col(f"net_consumption.{ContractColumnNames.metering_point_id}").alias(
                EphemeralColumnNames.net_consumption_metering_point_id
            ),
            F.col(f"net_consumption.{ContractColumnNames.coupled_date}").alias(
                EphemeralColumnNames.net_consumption_period_start
            ),
            F.col(f"net_consumption.{ContractColumnNames.uncoupled_date}").alias(
                EphemeralColumnNames.net_consumption_period_end
            ),
            F.col(f"consumption_from_grid.{ContractColumnNames.metering_point_id}").alias(
                EphemeralColumnNames.consumption_from_grid_metering_point_id
            ),
            F.col(f"consumption_from_grid.{ContractColumnNames.coupled_date}").alias(
                EphemeralColumnNames.consumption_from_grid_period_start
            ),
            F.col(f"consumption_from_grid.{ContractColumnNames.uncoupled_date}").alias(
                EphemeralColumnNames.consumption_from_grid_period_end
            ),
            F.col(f"supply_to_grid.{ContractColumnNames.metering_point_id}").alias(
                EphemeralColumnNames.supply_to_grid_metering_point_id
            ),
            F.col(f"supply_to_grid.{ContractColumnNames.coupled_date}").alias(
                EphemeralColumnNames.supply_to_grid_period_start
            ),
            F.col(f"supply_to_grid.{ContractColumnNames.uncoupled_date}").alias(
                EphemeralColumnNames.supply_to_grid_period_end
            ),
        )
    )


def _close_open_ended_periods(
    metering_point_periods: DataFrame,
    execution_start_datetime_local_time: datetime,
) -> DataFrame:
    """Close open ended periods by setting the end date to the end of the current settlement year."""
    settlement_year_end = "settlement_year_end"

    return metering_point_periods.select(
        "*",
        _settlement_year_end_datetime(
            F.col(ContractColumnNames.settlement_month), execution_start_datetime_local_time
        ).alias(settlement_year_end),
    ).select(
        # Consumption metering point
        F.col(ContractColumnNames.parent_metering_point_id),
        F.col(ContractColumnNames.net_settlement_group),
        F.col(ContractColumnNames.settlement_month),
        F.col(EphemeralColumnNames.parent_period_start),
        F.col(EphemeralColumnNames.parent_period_end),
        # Electrical heating metering point
        F.col(EphemeralColumnNames.electrical_heating_period_start),
        F.coalesce(
            F.col(EphemeralColumnNames.electrical_heating_period_end),
            settlement_year_end,
        ).alias(EphemeralColumnNames.electrical_heating_period_end),
        F.col(EphemeralColumnNames.electrical_heating_metering_point_id),
        # Net consumption metering point
        F.col(EphemeralColumnNames.net_consumption_period_start),
        F.coalesce(
            F.col(EphemeralColumnNames.net_consumption_period_end),
            settlement_year_end,
        ).alias(EphemeralColumnNames.net_consumption_period_end),
        F.col(EphemeralColumnNames.net_consumption_metering_point_id),
        # Consumption from grid metering point
        EphemeralColumnNames.consumption_from_grid_metering_point_id,
        EphemeralColumnNames.consumption_from_grid_period_start,
        F.coalesce(EphemeralColumnNames.consumption_from_grid_period_end, settlement_year_end).alias(
            EphemeralColumnNames.consumption_from_grid_period_end
        ),
        # Supply to grid metering point
        EphemeralColumnNames.supply_to_grid_metering_point_id,
        EphemeralColumnNames.supply_to_grid_period_start,
        F.coalesce(EphemeralColumnNames.supply_to_grid_period_end, settlement_year_end).alias(
            EphemeralColumnNames.supply_to_grid_period_end
        ),
    )


def _settlement_year_end_datetime(settlement_month: Column, execution_start_datetime_local_time: datetime) -> Column:
    """Return the end of the settlement year based on the settlement month (integer)."""
    temp = F.to_date(F.concat_ws("-", F.year(F.lit(execution_start_datetime_local_time)), settlement_month, F.lit("1")))
    return F.when(temp <= F.lit(execution_start_datetime_local_time), F.add_months(temp, 12)).otherwise(temp)


def _find_parent_child_overlap_period(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        # Here we calculate the overlapping period between the consumption metering point period
        # and the children metering point periods.
        # We, however, assume that there is only one overlapping period between the periods
        F.greatest(
            F.col(EphemeralColumnNames.parent_period_start),
            F.col(EphemeralColumnNames.electrical_heating_period_start),
            F.col(EphemeralColumnNames.net_consumption_period_start),
            F.col(EphemeralColumnNames.consumption_from_grid_period_start),
            F.col(EphemeralColumnNames.supply_to_grid_period_start),
        ).alias(EphemeralColumnNames.overlap_period_start_lt),
        F.least(
            F.col(EphemeralColumnNames.parent_period_end),
            F.col(EphemeralColumnNames.electrical_heating_period_end),
            F.col(EphemeralColumnNames.net_consumption_period_end),
            F.col(EphemeralColumnNames.consumption_from_grid_period_end),
            F.col(EphemeralColumnNames.supply_to_grid_period_end),
        ).alias(EphemeralColumnNames.overlap_period_end_lt),
    ).where(F.col(EphemeralColumnNames.overlap_period_start_lt) < F.col(EphemeralColumnNames.overlap_period_end_lt))


@testing()
def _split_period_by_settlement_year(
    parent_and_child_metering_point_and_periods: DataFrame, execution_start_datetime_local_time: datetime
) -> DataFrame:
    settlement_year_date = "settlement_year_date"

    return parent_and_child_metering_point_and_periods.select(
        "*",
        # create a row for each settlement year in the period
        F.explode(
            F.sequence(
                _beginning_of_settlement_year(
                    F.col(EphemeralColumnNames.parent_period_start), F.col(ContractColumnNames.settlement_month)
                ),
                F.coalesce(
                    # Subtract a tiny bit to avoid including the next year if the period ends at new year
                    _beginning_of_settlement_year(
                        F.expr(f"{EphemeralColumnNames.parent_period_end} - INTERVAL 1 SECOND"),
                        F.col(ContractColumnNames.settlement_month),
                    ),
                    F.add_months(
                        _beginning_of_settlement_year(
                            F.lit(execution_start_datetime_local_time), F.col(ContractColumnNames.settlement_month)
                        ),
                        12,
                    ),
                ),
                F.expr("INTERVAL 1 YEAR"),
            )
        ).alias(settlement_year_date),
    ).select(
        F.col(ContractColumnNames.parent_metering_point_id),
        F.col(ContractColumnNames.net_settlement_group),
        # When period starts withing the settlement year, use that date, otherwise use the settlement year start
        F.when(
            _is_in_settlement_year(F.col(EphemeralColumnNames.parent_period_start), F.col(settlement_year_date)),
            F.col(EphemeralColumnNames.parent_period_start),
        )
        .otherwise(F.col(settlement_year_date))
        .alias(EphemeralColumnNames.parent_period_start),
        F.when(
            _is_in_settlement_year(F.col(EphemeralColumnNames.parent_period_end), F.col(settlement_year_date)),
            F.col(EphemeralColumnNames.parent_period_end),
        )
        .otherwise(F.add_months(F.col(settlement_year_date), 12))
        .alias(EphemeralColumnNames.parent_period_end),
        F.col(EphemeralColumnNames.overlap_period_start_lt),
        F.col(EphemeralColumnNames.overlap_period_end_lt),
        F.col(settlement_year_date).alias(EphemeralColumnNames.settlement_month_datetime),
        F.col(EphemeralColumnNames.electrical_heating_metering_point_id),
        F.col(EphemeralColumnNames.net_consumption_metering_point_id),
        F.col(EphemeralColumnNames.consumption_from_grid_metering_point_id),
        F.col(EphemeralColumnNames.supply_to_grid_metering_point_id),
    )


def _is_in_settlement_year(date: Column, settlement_year_date: Column) -> Column:
    return (date >= settlement_year_date) & (date < F.add_months(settlement_year_date, 12))


def _beginning_of_settlement_year(period_start_date: Column, settlement_month: Column) -> Column:
    """Return the first date, which is the 1st of the settlement_month and is no later than period_start_date."""
    settlement_date = F.make_date(F.year(period_start_date), settlement_month, F.lit(1))
    return F.when(settlement_date <= period_start_date, settlement_date).otherwise(F.add_months(settlement_date, -12))


def _remove_net_settlement_group_2_up2end_without_netconsumption(
    parent_and_child_metering_point_and_periods: DataFrame, execution_start_datetime_local_time: datetime
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.where(
        ~(
            (
                F.col(ContractColumnNames.net_settlement_group).isNotNull()
                & (F.col(ContractColumnNames.net_settlement_group) == NetSettlementGroup.NET_SETTLEMENT_GROUP_2)
            )
            & F.col(EphemeralColumnNames.net_consumption_metering_point_id).isNull()
            # When current date is in the settlement year, then we're in a up-to-end period
            & _is_in_settlement_year(
                F.lit(execution_start_datetime_local_time), F.col(EphemeralColumnNames.settlement_month_datetime)
            )
        )
    )


def has_overlapping_period(
    period1_start: Column, period1_end: Column, period2_start: Column, period2_end: Column
) -> Column:
    return (period1_start < period2_end) & (period2_start < period1_end)
