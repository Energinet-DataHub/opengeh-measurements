from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark_functions.functions import (
    begining_of_year,
    convert_from_utc,
    convert_to_utc,
    days_in_year,
)
from telemetry_logging import use_span

import opengeh_electrical_heating.infrastructure.electricity_market as em
import opengeh_electrical_heating.infrastructure.measurements_gold as mg
from opengeh_electrical_heating.application.job_args.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from opengeh_electrical_heating.domain.constants import (
    CONSUMPTION_METERING_POINT_TYPE,
    ELECTRICAL_HEATING_LIMIT_YEARLY,
    NET_SETTLEMENT_GROUP_2,
)


@use_span()
def execute(spark: SparkSession, args: ElectricalHeatingArgs) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = em.Repository(spark, args.catalog_name)
    measurements_gold_repository = mg.Repository(spark, args.catalog_name)

    # Read data frames
    consumption_metering_point_periods = electricity_market_repository.read_consumption_metering_point_periods()
    child_metering_points = electricity_market_repository.read_child_metering_points()
    time_series_points = measurements_gold_repository.read_time_series_points()

    # Execute the calculation logic
    execute_core_logic(
        time_series_points,
        consumption_metering_point_periods,
        child_metering_points,
        args.time_zone,
    )


# This is a temporary implementation. The final implementation will be provided in later PRs.
# This is also the function that will be tested using the `testcommon.etl` framework.
@use_span()
def execute_core_logic(
    time_series_points: DataFrame,
    consumption_metering_point_periods: DataFrame,
    child_metering_points: DataFrame,
    time_zone: str,
) -> DataFrame:
    consumption = time_series_points.where(
        (F.col("metering_point_type") == CONSUMPTION_METERING_POINT_TYPE)
        | (F.col("metering_point_type") == em.MeteringPointType.NET_CONSUMPTION.value)
    )
    electrical_heating = time_series_points.where(
        F.col("metering_point_type") == em.MeteringPointType.ELECTRICAL_HEATING.value
    )

    parent_metering_points = convert_from_utc(consumption_metering_point_periods, time_zone)
    child_metering_points = convert_from_utc(child_metering_points, time_zone)
    consumption = convert_from_utc(consumption, time_zone)
    electrical_heating = convert_from_utc(electrical_heating, time_zone)

    # prepare child metering points and parent metering points
    metering_point_periods = _join_children_to_parent_metering_point(child_metering_points, parent_metering_points)
    metering_point_periods = _close_open_ended_periods(metering_point_periods)
    metering_point_periods = _find_parent_child_overlap_period(metering_point_periods)
    metering_point_periods = _split_consumption_period_by_year(metering_point_periods)
    metering_point_periods = _calculate_period_consumption_limit(metering_point_periods)

    # prepare consumption and electrical heating time series data
    consumption_daily = _calculate_daily_quantity(consumption)
    previous_electrical_heating = _calculate_daily_quantity(electrical_heating)

    # determine from which metering point to get the consumption data (consumption or net consumption)
    metering_point_periods = _find_source_metering_point_for_consumption(metering_point_periods)

    # here conumption time series and metering point periods data is joined
    consumption_with_metering_point_periods = _join_source_metering_point_periods_with_consumption(
        consumption_daily,
        metering_point_periods,
    )
    consumption_with_metering_point_periods.show()
    consumption = _filter_parent_child_overlap_period_and_year(consumption_with_metering_point_periods)
    consumption = _aggregate_quantity_over_period(consumption)
    electrical_heating = _impose_period_quantity_limit(consumption)
    electrical_heating = _filter_unchanged_electrical_heating(electrical_heating, previous_electrical_heating)

    electrical_heating = convert_to_utc(electrical_heating, time_zone)

    return electrical_heating.orderBy(F.col("metering_point_id"), F.col("date"))


def _filter_unchanged_electrical_heating(
    newly_calculated_electrical_heating: DataFrame,
    electrical_heating_from_before: DataFrame,
) -> DataFrame:
    return (
        newly_calculated_electrical_heating.alias("current")
        .join(
            electrical_heating_from_before.alias("previous"),
            (
                (F.col("current.metering_point_id") == F.col("previous.metering_point_id"))
                & (F.col("current.date") == F.col("previous.date"))
                & (F.col("current.quantity") == F.col("previous.quantity"))
            ),
            "left_anti",
        )
        .select(
            F.col("current.metering_point_id"),
            F.col("current.date"),
            F.col("current.quantity"),
        )
    )


def _impose_period_quantity_limit(time_series_consumption: DataFrame) -> DataFrame:
    return time_series_consumption.select(
        F.when(
            (F.col("cumulative_quantity") >= F.col("period_consumption_limit"))
            & (F.col("cumulative_quantity") - F.col("quantity") < F.col("period_consumption_limit")),
            F.col("period_consumption_limit") + F.col("quantity") - F.col("cumulative_quantity"),
        )
        .when(
            F.col("cumulative_quantity") > F.col("period_consumption_limit"),
            0,
        )
        .otherwise(
            F.col("quantity"),
        )
        .cast(T.DecimalType(38, 3))
        .alias("quantity"),
        F.col("cumulative_quantity"),
        F.col("metering_point_id"),
        F.col("date"),
        F.col("period_consumption_limit"),
    ).drop_duplicates()


def _aggregate_quantity_over_period(time_series_consumption: DataFrame) -> DataFrame:
    period_window = (
        Window.partitionBy(
            F.col("metering_point_id"),
            F.col("parent_period_start"),
            F.col("parent_period_end"),
        )
        .orderBy(F.col("date"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return time_series_consumption.select(
        F.sum(F.col("quantity")).over(period_window).alias("cumulative_quantity"),
        F.col("metering_point_id"),
        F.col("date"),
        F.col("quantity"),
        F.col("period_consumption_limit"),
    ).drop_duplicates()


def _filter_parent_child_overlap_period_and_year(
    time_series_consumption: DataFrame,
) -> DataFrame:
    return time_series_consumption.where(
        (F.col("date") >= F.col("overlap_period_start"))
        & (F.col("date") < F.col("overlap_period_end"))
        & (F.year(F.col("date")) == F.year(F.col("period_year")))
    )


def _join_source_metering_point_periods_with_consumption(
    time_series_consumption: DataFrame,
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return (
        time_series_consumption.alias("consumption")
        .join(
            parent_and_child_metering_point_and_periods.alias("metering_point"),
            # source_metering_point_id is either D14 or D15 depending on the net settlement group
            F.col("consumption.metering_point_id") == F.col("metering_point.source_metering_point_id"),
            "inner",
        )
        .select(
            F.col("metering_point.parent_period_start").alias("parent_period_start"),
            F.col("metering_point.parent_period_end").alias("parent_period_end"),
            F.col("metering_point.d14_metering_point_id").alias("metering_point_id"),
            F.col("consumption.date").alias("date"),
            F.col("consumption.quantity").alias("quantity"),
            F.col("metering_point.period_consumption_limit").alias("period_consumption_limit"),
            F.col("metering_point.overlap_period_start").alias("overlap_period_start"),
            F.col("metering_point.overlap_period_end").alias("overlap_period_end"),
        )
    )


def _calculate_daily_quantity(time_series: DataFrame) -> DataFrame:
    daily_window = Window.partitionBy(
        F.col("metering_point_id"),
        F.col("date"),
    )

    return (
        time_series.select(
            "*",
            F.date_trunc("day", F.col("observation_time")).alias("date"),
        )
        .select(
            F.sum(F.col("quantity")).over(daily_window).alias("quantity"),
            F.col("date"),
            F.col("metering_point_id"),
        )
        .drop_duplicates()
    )


def _find_source_metering_point_for_consumption(metering_point_periods: DataFrame) -> DataFrame:
    """Determine which metering point to use for consumption data.
    - For net settlement group 2: use the net consumption metering point
    - For other: use the consumption metering point (this will be updated when more net settlement groups are added)

    The metering point id is added as a column named `source_metering_point_id`.
    """
    return metering_point_periods.select(
        "*",
        F.when(
            F.col("parent_net_settlement_group") == NET_SETTLEMENT_GROUP_2,
            F.col("d15_metering_point_id"),
        )
        .otherwise(F.col("parent_metering_point_id"))
        .alias("source_metering_point_id"),
    )


def _calculate_period_consumption_limit(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        (
            F.datediff(F.col("parent_period_end"), F.col("parent_period_start"))
            * ELECTRICAL_HEATING_LIMIT_YEARLY
            / days_in_year(F.col("parent_period_start"))
        ).alias("period_consumption_limit"),
    )


def _split_consumption_period_by_year(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        # create a row for each year in the consumption period
        F.explode(
            F.sequence(
                begining_of_year(F.col("parent_period_start")),
                F.coalesce(
                    begining_of_year(F.col("parent_period_end")),
                    begining_of_year(F.current_date(), years_to_add=1),
                ),
                F.expr("INTERVAL 1 YEAR"),
            )
        ).alias("period_year"),
    ).select(
        F.col("parent_metering_point_id"),
        F.col("parent_net_settlement_group"),
        F.when(
            F.year(F.col("parent_period_start")) == F.year(F.col("period_year")),
            F.col("parent_period_start"),
        )
        .otherwise(begining_of_year(date=F.col("period_year")))
        .alias("parent_period_start"),
        F.when(
            F.year(F.col("parent_period_end")) == F.year(F.col("period_year")),
            F.col("parent_period_end"),
        )
        .otherwise(begining_of_year(date=F.col("period_year"), years_to_add=1))
        .alias("parent_period_end"),
        F.col("overlap_period_start"),
        F.col("overlap_period_end"),
        F.col("period_year"),
        F.col("d14_metering_point_id"),
        F.col("d14_period_start"),
        F.col("d14_period_end"),
        F.col("d15_metering_point_id"),
        F.col("d15_period_start"),
        F.col("d15_period_end"),
    )


def _find_parent_child_overlap_period(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return parent_and_child_metering_point_and_periods.select(
        "*",
        # here we calculate the overlaping period between the consumption period and the children periods
        # we however assume that there is only one overlapping period between the periods
        F.greatest(
            F.col("parent_period_start"),
            F.col("d14_period_start"),
            F.col("d15_period_start"),
        ).alias("overlap_period_start"),
        F.least(
            F.coalesce(
                F.col("parent_period_end"),
                begining_of_year(F.current_date(), years_to_add=1),
            ),
            F.coalesce(
                F.col("d14_period_end"),
                begining_of_year(F.current_date(), years_to_add=1),
            ),
            F.coalesce(
                F.col("d15_period_end"),
                begining_of_year(F.current_date(), years_to_add=1),
            ),
        ).alias("overlap_period_end"),
    ).where(F.col("overlap_period_start") < F.col("overlap_period_end"))


def _close_open_ended_periods(
    parent_and_child_metering_point_and_periods: DataFrame,
) -> DataFrame:
    """Close open ended periods by setting the end date to the end of the current year."""
    return parent_and_child_metering_point_and_periods.select(
        # E17
        F.col("parent_metering_point_id"),
        F.col("parent_net_settlement_group"),
        F.col("parent_period_start"),
        F.coalesce(
            F.col("parent_period_end"),
            begining_of_year(F.current_date(), years_to_add=1),
        ).alias("parent_period_end"),
        # D14
        F.col("d14_period_start"),
        F.coalesce(
            F.col("d14_period_end"),
            begining_of_year(F.current_date(), years_to_add=1),
        ).alias("d14_period_end"),
        F.col("d14_metering_point_id"),
        # D15
        F.col("d15_period_start"),
        F.coalesce(
            F.col("d15_period_end"),
            begining_of_year(F.current_date(), years_to_add=1),
        ).alias("d15_period_end"),
        F.col("d15_metering_point_id"),
    )


def _join_children_to_parent_metering_point(
    child_metering_point_and_periods: DataFrame,
    parent_metering_point_and_periods: DataFrame,
) -> DataFrame:
    return (
        parent_metering_point_and_periods.alias("parent")
        .join(
            child_metering_point_and_periods.alias("d14"),
            F.col("d14.parent_metering_point_id") == F.col("parent.metering_point_id"),
            "inner",
        )
        .join(
            child_metering_point_and_periods.alias("d15"),
            F.col("d15.metering_point_id") == F.col("parent.metering_point_id"),
            "left",
        )
        .select(
            F.col("parent.metering_point_id").alias("parent_metering_point_id"),
            F.col("parent.net_settlement_group").alias("parent_net_settlement_group"),
            F.col("parent.period_from_date").alias("parent_period_start"),
            F.col("parent.period_to_date").alias("parent_period_end"),
            F.col("d14.metering_point_id").alias("d14_metering_point_id"),
            F.col("d14.coupled_date").alias("d14_period_start"),
            F.col("d14.uncoupled_date").alias("d14_period_end"),
            F.col("d15.metering_point_id").alias("d15_metering_point_id"),
            F.col("d15.coupled_date").alias("d15_period_start"),
            F.col("d15.uncoupled_date").alias("d15_period_end"),
        )
    )
