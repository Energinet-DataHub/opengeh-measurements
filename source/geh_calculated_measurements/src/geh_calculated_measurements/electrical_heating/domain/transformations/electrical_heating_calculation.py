from datetime import datetime
from zoneinfo import ZoneInfo

from geh_common.domain.types import NetSettlementGroup
from geh_common.testing.dataframes import testing
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.electrical_heating.domain.ephemeral_column_names import EphemeralColumnNames

_ELECTRICAL_HEATING_LIMIT_YEARLY = 4000.0
"""Limit in kWh."""


def calculate_electrical_heating_in_local_time(
    time_series_points_hourly: DataFrame, periods: DataFrame, time_zone: str, execution_start_datetime: datetime
) -> DataFrame:
    execution_start_datetime_local_time = execution_start_datetime.astimezone(ZoneInfo(time_zone))

    periods = _find_source_metering_point_for_energy(periods)

    print("periods - table")
    periods.show()
    print("periods - schema")
    periods.printSchema()

    periods_with_hourly_energy = _join_source_metering_point_periods_with_energy_hourly(
        periods,
        time_series_points_hourly,
    )

    print("periods_with_hourly_energy - table")
    periods_with_hourly_energy.show()
    print("periods_with_hourly_energy - schema")
    periods.printSchema()

    periods_with_daily_energy_and_limit = _calculate_period_limit(
        periods_with_hourly_energy, execution_start_datetime_local_time
    )

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
                F.col(ContractColumnNames.net_settlement_group).isin(
                    NetSettlementGroup.NET_SETTLEMENT_GROUP_2, NetSettlementGroup.NET_SETTLEMENT_GROUP_6
                )
            )
            & (F.col(EphemeralColumnNames.net_consumption_metering_point_id).isNotNull()),
            F.col(EphemeralColumnNames.net_consumption_metering_point_id),
        )
        .otherwise(F.col(ContractColumnNames.parent_metering_point_id))
        .alias(EphemeralColumnNames.energy_source_metering_point_id),
    )


@testing()
def _join_source_metering_point_periods_with_energy_hourly(
    parent_and_child_metering_point_and_periods_in_localtime: DataFrame,
    time_series_points_hourly: DataFrame,
) -> DataFrame:
    merge_on_consumption = (
        parent_and_child_metering_point_and_periods_in_localtime.alias("metering_point")
        # Join each (net)consumption metering point with the (net)consumption time series points
        # Inner join to only include metering points with energy data.
        .join(
            time_series_points_hourly.alias("consumption"),
            (
                F.col(f"consumption.{ContractColumnNames.metering_point_id}")
                == F.col(f"metering_point.{EphemeralColumnNames.energy_source_metering_point_id}")
            )
            & (
                F.col(f"consumption.{EphemeralColumnNames.observation_time_hourly_lt}")
                >= F.col(f"metering_point.{EphemeralColumnNames.overlap_period_start_lt}")
            )
            & (
                F.col(f"consumption.{EphemeralColumnNames.observation_time_hourly_lt}")
                < F.col(f"metering_point.{EphemeralColumnNames.overlap_period_end_lt}")
            )
            & (
                (
                    F.col(f"consumption.{EphemeralColumnNames.observation_time_hourly_lt}")
                    >= F.col(f"metering_point.{EphemeralColumnNames.settlement_month_datetime}")
                )
                & (
                    F.col(f"consumption.{EphemeralColumnNames.observation_time_hourly_lt}")
                    < F.add_months(F.col(f"metering_point.{EphemeralColumnNames.settlement_month_datetime}"), 12)
                )
            ),
            "inner",
        )
    )

    print("merge_on_consumption - table")
    merge_on_consumption.show()
    print("merge_on_consumption - schema")
    merge_on_consumption.printSchema()

    merge_on_grid_supply = merge_on_consumption.join(
        time_series_points_hourly.alias("supply_to_grid"),
        (
            F.col(f"supply_to_grid.{ContractColumnNames.metering_point_id}")
            == F.col(f"metering_point.{EphemeralColumnNames.supply_to_grid_metering_point_id}")
        )
        & (
            F.col(f"supply_to_grid.{EphemeralColumnNames.observation_time_hourly}")
            == F.col(f"consumption.{EphemeralColumnNames.observation_time_hourly}")
        ),
        "left",
    )
    merge_on_grid_consumption = merge_on_grid_supply.join(
        time_series_points_hourly.alias("consumption_from_grid"),
        (
            F.col(f"consumption_from_grid.{ContractColumnNames.metering_point_id}")
            == F.col(f"metering_point.{EphemeralColumnNames.consumption_from_grid_metering_point_id}")
        )
        & (
            F.col(f"consumption_from_grid.{EphemeralColumnNames.observation_time_hourly}")
            == F.col(f"consumption.{EphemeralColumnNames.observation_time_hourly}")
        ),
        "left",
    )

    select_columns = merge_on_grid_consumption.select(
        F.col(f"metering_point.{EphemeralColumnNames.parent_period_start}").alias(
            EphemeralColumnNames.parent_period_start
        ),
        F.col(f"metering_point.{EphemeralColumnNames.parent_period_end}").alias(EphemeralColumnNames.parent_period_end),
        F.col(f"metering_point.{ContractColumnNames.net_settlement_group}").alias(
            ContractColumnNames.net_settlement_group
        ),
        F.col(f"metering_point.{EphemeralColumnNames.settlement_month_datetime}").alias(
            EphemeralColumnNames.settlement_month_datetime
        ),
        F.col(f"metering_point.{EphemeralColumnNames.electrical_heating_metering_point_id}").alias(
            EphemeralColumnNames.electrical_heating_metering_point_id
        ),
        F.col(f"consumption.{EphemeralColumnNames.observation_time_hourly_lt}").alias(
            EphemeralColumnNames.observation_time_hourly_lt
        ),
        F.col(f"consumption.{ContractColumnNames.quantity}").alias(ContractColumnNames.quantity),
        F.col(f"supply_to_grid.{ContractColumnNames.metering_point_id}").alias(
            EphemeralColumnNames.supply_to_grid_metering_point_id
        ),
        F.col(f"supply_to_grid.{ContractColumnNames.quantity}").alias(EphemeralColumnNames.supply_to_grid_quantity),
        F.col(f"consumption_from_grid.{ContractColumnNames.metering_point_id}").alias(
            EphemeralColumnNames.consumption_from_grid_metering_point_id
        ),
        F.col(f"consumption_from_grid.{ContractColumnNames.quantity}").alias(
            EphemeralColumnNames.consumption_from_grid_quantity
        ),
    )
    return select_columns


@testing()
def _calculate_period_limit(  # for nsg 2 and 6 we only lets end of period through
    periods_with_hourly_energy: DataFrame, execution_start_datetime_local_time: datetime
) -> DataFrame:
    MONTHS_IN_YEAR = 12
    periods_with_hourly_energy = _calculate_base_period_limit(periods_with_hourly_energy)

    periods_with_hourly_energy = (
        periods_with_hourly_energy.select(  # should be if: the period is closed | in ended SMRD
            "*",
            (
                (
                    F.add_months(F.col(EphemeralColumnNames.settlement_month_datetime), MONTHS_IN_YEAR)
                    <= F.lit(execution_start_datetime_local_time)
                )
                | (F.col(EphemeralColumnNames.parent_period_end) < F.lit(execution_start_datetime_local_time))
            ).alias("is_end_of_period"),
        )
    )

    # Prevent multiple evaluations of this expensive data frame
    periods_with_hourly_energy.cache()

    no_nsg_or_up2end = _calculate_period_limit__no_net_settlement_group_or_up2end(  # why we set non nsg 2 and 6 set to up to end? - but not relevant for this test
        periods_with_hourly_energy.where(
            (
                F.col(ContractColumnNames.net_settlement_group).isNull()
                | ~F.col(ContractColumnNames.net_settlement_group).isin(2, 6)
            )
            | ~F.col("is_end_of_period")
        )
    ).withColumn("is_end_of_period", F.lit(False))

    nsg2_end_of_period = _calculate_period_limit__net_settlement_group_2_end_of_period(
        periods_with_hourly_energy.where(
            (F.col(ContractColumnNames.net_settlement_group) == NetSettlementGroup.NET_SETTLEMENT_GROUP_2)
            & F.col("is_end_of_period")
        )
    ).withColumn("is_end_of_period", F.lit(True))

    nsg6_end_of_period = _calculate_period_limit__net_settlement_group_6_end_of_period(
        periods_with_hourly_energy.where(
            (F.col(ContractColumnNames.net_settlement_group) == NetSettlementGroup.NET_SETTLEMENT_GROUP_6)
            & F.col("is_end_of_period")
        )
    ).withColumn("is_end_of_period", F.lit(True))

    periods_with_daily_energy_and_limit = no_nsg_or_up2end.union(nsg2_end_of_period).union(nsg6_end_of_period)

    return periods_with_daily_energy_and_limit


@testing()
def _calculate_base_period_limit(periods_with_energy_hourly: DataFrame) -> DataFrame:
    return periods_with_energy_hourly.select(
        "*",
        (
            F.datediff(F.col(EphemeralColumnNames.parent_period_end), F.col(EphemeralColumnNames.parent_period_start))
            * _ELECTRICAL_HEATING_LIMIT_YEARLY
            / _days_in_settlement_year(F.col(EphemeralColumnNames.settlement_month_datetime))
        ).alias(EphemeralColumnNames.base_period_limit),
    )


@testing()
def _calculate_period_limit__no_net_settlement_group_or_up2end(
    periods_with_energy_hourly: DataFrame,
) -> DataFrame:
    return periods_with_energy_hourly.groupBy(
        EphemeralColumnNames.electrical_heating_metering_point_id,
        EphemeralColumnNames.parent_period_start,
        EphemeralColumnNames.parent_period_end,
        F.date_trunc("day", F.col(EphemeralColumnNames.observation_time_hourly_lt)).alias(ContractColumnNames.date),
    ).agg(
        F.sum(ContractColumnNames.quantity).alias(ContractColumnNames.quantity),
        F.first(EphemeralColumnNames.base_period_limit).alias(EphemeralColumnNames.period_energy_limit),
    )


@testing()
def _calculate_period_limit__net_settlement_group_2_end_of_period(
    periods_with_energy_hourly: DataFrame,
) -> DataFrame:
    """Calculate the period limit.

    For net settlement group 2: Both up to end and end of period."
    For net settlement group 6: Only up 2 end of period."
    """
    df = periods_with_energy_hourly.select(
        "*",
        F.coalesce(
            F.least(
                F.col(EphemeralColumnNames.consumption_from_grid_quantity),
                F.col(EphemeralColumnNames.supply_to_grid_quantity),
            ),
            F.lit(0),
        ).alias("nettoficated_hourly"),
    )

    period_window = Window.partitionBy(
        F.col(EphemeralColumnNames.electrical_heating_metering_point_id),
        F.col(EphemeralColumnNames.parent_period_start),
        F.col(EphemeralColumnNames.parent_period_end),
    )

    return (
        df.groupBy(
            EphemeralColumnNames.electrical_heating_metering_point_id,
            EphemeralColumnNames.parent_period_start,
            EphemeralColumnNames.parent_period_end,
            F.date_trunc("day", F.col(EphemeralColumnNames.observation_time_hourly_lt)).alias(ContractColumnNames.date),
        )
        .agg(
            F.sum(F.col(ContractColumnNames.quantity)).alias(ContractColumnNames.quantity),
            F.sum(F.col("nettoficated_hourly")).alias("nettoficated_daily"),
            F.first(EphemeralColumnNames.settlement_month_datetime).alias(
                EphemeralColumnNames.settlement_month_datetime
            ),
            F.first(EphemeralColumnNames.base_period_limit).alias(EphemeralColumnNames.base_period_limit),
        )
        .select(
            "*",
            F.sum(F.col("nettoficated_daily")).over(period_window).alias("period_limit_reduction"),
        )
        .select(
            EphemeralColumnNames.electrical_heating_metering_point_id,
            EphemeralColumnNames.parent_period_start,
            EphemeralColumnNames.parent_period_end,
            ContractColumnNames.date,
            ContractColumnNames.quantity,
            (
                F.greatest(F.col(EphemeralColumnNames.base_period_limit) - F.col("period_limit_reduction"), F.lit(0))
            ).alias(EphemeralColumnNames.period_energy_limit),
        )
    )


@testing()
def _calculate_period_limit__net_settlement_group_6_end_of_period(
    periods_with_energy_hourly: DataFrame,
) -> DataFrame:
    # Window used to aggregate values for the whole period
    period_window = Window.partitionBy(
        F.col(EphemeralColumnNames.electrical_heating_metering_point_id),
        F.col(EphemeralColumnNames.parent_period_start),
        F.col(EphemeralColumnNames.parent_period_end),
    )

    return (
        # Calculate the period limit reduction
        periods_with_energy_hourly.select(
            "*",
            (
                F.least(
                    F.sum(F.col(EphemeralColumnNames.consumption_from_grid_quantity)).over(period_window),
                    F.sum(F.col(EphemeralColumnNames.supply_to_grid_quantity)).over(period_window),
                )
            ).alias("period_limit_reduction"),
        )
        # Then aggregate to a row per day
        .groupBy(
            EphemeralColumnNames.electrical_heating_metering_point_id,
            EphemeralColumnNames.parent_period_start,
            EphemeralColumnNames.parent_period_end,
            F.date_trunc("day", F.col(EphemeralColumnNames.observation_time_hourly_lt)).alias(ContractColumnNames.date),
        )
        # Calculate daily values + keep period values (using F.first())
        .agg(
            F.sum(F.col(ContractColumnNames.quantity)).alias(ContractColumnNames.quantity),
            F.greatest(
                (F.first(EphemeralColumnNames.base_period_limit) - F.first("period_limit_reduction")), F.lit(0)
            ).alias(EphemeralColumnNames.period_energy_limit),
            F.first(EphemeralColumnNames.settlement_month_datetime).alias(
                EphemeralColumnNames.settlement_month_datetime
            ),
        )
        .select(
            EphemeralColumnNames.electrical_heating_metering_point_id,
            EphemeralColumnNames.parent_period_start,
            EphemeralColumnNames.parent_period_end,
            ContractColumnNames.date,
            ContractColumnNames.quantity,
            EphemeralColumnNames.period_energy_limit,
        )
    )


def _days_in_settlement_year(settlement_month_datetime: Column) -> Column:
    return F.datediff(F.add_months(settlement_month_datetime, 12), settlement_month_datetime)


def _aggregate_quantity_over_period(time_series_points: DataFrame) -> DataFrame:
    period_window = (
        Window.partitionBy(
            F.col(EphemeralColumnNames.electrical_heating_metering_point_id),
            F.col(EphemeralColumnNames.parent_period_start),
            F.col(EphemeralColumnNames.parent_period_end),
        )
        .orderBy(F.col(ContractColumnNames.date))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return time_series_points.select(
        F.sum(F.col(ContractColumnNames.quantity)).over(period_window).alias(EphemeralColumnNames.cumulative_quantity),
        F.col(EphemeralColumnNames.electrical_heating_metering_point_id),
        F.col(ContractColumnNames.date),
        F.col(ContractColumnNames.quantity),
        F.col(EphemeralColumnNames.period_energy_limit),
        F.col("is_end_of_period"),
    )


def _impose_period_quantity_limit(time_series_points: DataFrame) -> DataFrame:
    return time_series_points.select(
        F.when(
            (F.col(EphemeralColumnNames.cumulative_quantity) >= F.col(EphemeralColumnNames.period_energy_limit))
            & (
                F.col(EphemeralColumnNames.cumulative_quantity) - F.col(ContractColumnNames.quantity)
                < F.col(EphemeralColumnNames.period_energy_limit)
            ),
            F.col(EphemeralColumnNames.period_energy_limit)
            + F.col(ContractColumnNames.quantity)
            - F.col(EphemeralColumnNames.cumulative_quantity),
        )
        .when(
            F.col(EphemeralColumnNames.cumulative_quantity) > F.col(EphemeralColumnNames.period_energy_limit),
            0,
        )
        .otherwise(
            F.col(ContractColumnNames.quantity),
        )
        .cast(DecimalType(18, 3))
        .alias(ContractColumnNames.quantity),
        F.col(EphemeralColumnNames.cumulative_quantity),
        F.col(EphemeralColumnNames.electrical_heating_metering_point_id).alias(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.date),
        F.col(EphemeralColumnNames.period_energy_limit),
        F.col("is_end_of_period"),
    )
