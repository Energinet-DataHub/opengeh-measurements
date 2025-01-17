from pyspark.sql import functions as F, types as T
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from telemetry_logging import use_span

import source.electrical_heating.src.electrical_heating.infrastructure.electricity_market as em
import source.electrical_heating.src.electrical_heating.infrastructure.measurements_gold as mg
from source.electrical_heating.src.electrical_heating.application.job_args.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from source.electrical_heating.src.electrical_heating.domain.constants import (
    ELECTRICAL_HEATING_LIMIT_YEARLY,
    ELECTRICAL_HEATING_METERING_POINT_TYPE,
)
from source.electrical_heating.src.electrical_heating.domain.pyspark_functions import (
    convert_from_utc,
    convert_to_utc,
    begining_of_year,
    days_in_year,
)


@use_span()
def execute(spark: SparkSession, args: ElectricalHeatingArgs) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = em.Repository(spark, args.catalog_name)
    measurements_gold_repository = mg.Repository(spark, args.catalog_name)

    # Read data frames
    consumption_metering_point_periods = (
        electricity_market_repository.read_consumption_metering_point_periods()
    )
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
    child_metering_points = child_metering_points.where(
        F.col(em.ColumnNames.metering_point_type)
        == em.MeteringPointType.ELECTRICAL_HEATING.value
    )
    consumption_metering_point_periods = convert_from_utc(
        consumption_metering_point_periods, time_zone
    )
    child_metering_point_periods = convert_from_utc(child_metering_points, time_zone)
    time_series_points = convert_from_utc(time_series_points, time_zone)

    # prepare child metering points and parent metering points

    join_child_to_parent_metering_point_df = join_child_to_parent_metering_point(
        consumption_metering_point_periods, child_metering_point_periods
    )
    handle_null_in_to_date_columns_df = handle_null_in_to_date_columns(
        join_child_to_parent_metering_point_df
    )
    find_parent_child_overlap_period_df = find_parent_child_overlap_period(
        handle_null_in_to_date_columns_df
    )
    split_consumption_period_by_year_df = split_consumption_period_by_year(
        find_parent_child_overlap_period_df
    )
    calculate_period_consumption_limit_df = calculate_period_consumption_limit(
        split_consumption_period_by_year_df
    )

    # prepare consumption time series data

    # for aggreating comsumption from every 15 minutes to daily

    consumption_time_series_to_daily_df = consumption_time_series_to_daily(
        time_series_points
    )

    unique_child_parent_metering_id_df = unique_child_parent_metering_id(
        child_metering_point_periods
    )

    join_unique_child_and_parent_id_to_consumption_daily_df = (
        join_unique_child_and_parent_id_to_consumption_daily(
            consumption_time_series_to_daily_df, unique_child_parent_metering_id_df
        )
    )

    non_calculated_consumption_df = non_calculated_consumption(
        join_unique_child_and_parent_id_to_consumption_daily_df
    )

    previously_calculated_consumption_df = previously_calculated_consumption(
        join_unique_child_and_parent_id_to_consumption_daily_df
    )

    join_consumption_to_previously_calculated_consumption_df = (
        join_consumption_to_previously_calculated_consumption(
            non_calculated_consumption_df, previously_calculated_consumption_df
        )
    )

    # here conumption time series and metering points data is joined
    join_consumption_to_metering_points = (
        join_consumption_to_previously_calculated_consumption.alias("consumption")
        .join(
            calculate_period_consumption_limit.alias("metering_point"),
            F.col("consumption.metering_point_id")
            == F.col("metering_point.parent_metering_point_id"),
            "inner",
        )
        .select(
            F.col("metering_point.parent_period_start").alias("parent_period_start"),
            F.col("metering_point.parent_period_end").alias("parent_period_end"),
            F.col("metering_point.child_metering_point_id").alias("metering_point_id"),
            F.col("consumption.date").alias("date"),
            F.col("consumption.quantity").alias("quantity"),
            F.col("metering_point.period_consumption_limit").alias(
                "period_consumption_limit"
            ),
            F.col("consumption.previously_calculated_quantity").alias(
                "previously_calculated_quantity"
            ),
            F.col("metering_point.parent_child_overlap_period_start").alias(
                "parent_child_overlap_period_start"
            ),
            F.col("metering_point.parent_child_overlap_period_end").alias(
                "parent_child_overlap_period_end"
            ),
        )
    )
    filter_for_parent_child_overlap_period_and_year = (
        join_consumption_to_metering_points.where(
            (F.col("date") >= F.col("parent_child_overlap_period_start"))
            & (F.col("date") < F.col("metering_point.parent_child_overlap_period_end"))
            & (F.year(F.col("date")) == F.year(F.col("period_year")))
        )
    )

    period_window = (
        Window.partitionBy(
            F.col("metering_point_id"),
            F.col("parent_period_start"),
            F.col("parent_period_end"),
        )
        .orderBy(F.col("date"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    period_consumption = filter_for_parent_child_overlap_period_and_year.select(
        F.sum(F.col("quantity")).over(period_window).alias("cumulative_quantity"),
        F.col("metering_point_id"),
        F.col("date"),
        F.col("quantity"),
        F.col("period_consumption_limit"),
        F.col("previously_calculated_quantity"),
    ).drop_duplicates()

    period_consumption_with_limit = period_consumption.select(
        F.when(
            (F.col("cumulative_quantity") >= F.col("period_consumption_limit"))
            & (
                F.col("cumulative_quantity") - F.col("quantity")
                < F.col("period_consumption_limit")
            ),
            F.col("period_consumption_limit")
            + F.col("quantity")
            - F.col("cumulative_quantity"),
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
        F.col("previously_calculated_quantity"),
    ).drop_duplicates()

    compare_previous_calculated_daily_consumption = period_consumption_with_limit.where(
        (
            (F.col("quantity") != F.col("previously_calculated_quantity"))
            | F.col("previously_calculated_quantity").isNull()
        )
    ).select(
        F.col("metering_point_id"),
        F.col("date"),
        F.col("quantity"),
    )

    compare_previous_calculated_daily_consumption = convert_to_utc(
        compare_previous_calculated_daily_consumption, time_zone
    )

    return compare_previous_calculated_daily_consumption


def join_consumption_to_previously_calculated_consumption(df1, df2):
    return (
        df1.alias("consumption")
        .join(
            df2.alias("previous"),
            (
                (
                    F.col("consumption.child_metering_point_id")
                    == F.col("previous.metering_point_id")
                )
                & (F.col("consumption.date") == F.col("previous.date"))
            ),
            "left",
        )
        .select(
            F.col("consumption.metering_point_id").alias("metering_point_id"),
            F.col("consumption.date").alias("date"),
            F.col("consumption.quantity").alias("quantity"),
            F.col("previous.quantity").alias("previously_calculated_quantity"),
        )
    )


def previously_calculated_consumption(df: DataFrame) -> DataFrame:
    return df.where(F.col("child_metering_point_id").isNull()).select(
        F.col("metering_point_id"),
        F.col("date"),
        F.col("quantity"),
    )


def non_calculated_consumption(df: DataFrame) -> DataFrame:
    return df.where(F.col("child_metering_point_id").isNotNull())


def join_unique_child_and_parent_id_to_consumption_daily(
    df1: DataFrame, df2: DataFrame
) -> DataFrame:
    return (
        df1.alias("consumption")
        .join(
            df2.alias("id"),
            F.col("consumption.metering_point_id")
            == F.col("id.parent_metering_point_id"),
            "left",
        )
        .select(
            F.col("consumption.metering_point_id").alias("metering_point_id"),
            F.col("consumption.date").alias("date"),
            F.col("consumption.quantity").alias("quantity"),
            F.col("id.child_metering_point_id").alias("child_metering_point_id"),
        )
    )


def unique_child_parent_metering_id(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("metering_point_id").alias("child_metering_point_id"),
        F.col("parent_metering_point_id"),
    ).drop_duplicates()


daily_window = Window.partitionBy(
    F.col("metering_point_id"),
    F.col("date"),
)


def consumption_time_series_to_daily(df: DataFrame) -> DataFrame:
    return (
        df.select(
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


def calculate_period_consumption_limit(df: DataFrame) -> DataFrame:
    return df.select(
        "*",
        (
            F.datediff(F.col("parent_period_end"), F.col("parent_period_start"))
            * ELECTRICAL_HEATING_LIMIT_YEARLY
            / days_in_year(F.col("parent_period_start"))
        ).alias("period_consumption_limit"),
    )


def split_consumption_period_by_year(df: DataFrame) -> DataFrame:
    return df.select(
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
        F.col("child_metering_point_id"),
        F.col("parent_metering_point_id"),
        F.col("parent_child_overlap_period_start"),
        F.col("parent_child_overlap_period_end"),
        F.col("period_year"),
        F.col("child_period_start"),
        F.col("child_period_end"),
    )


def find_parent_child_overlap_period(df: DataFrame) -> DataFrame:
    return (
        df.select(
            "*",
            # here we calculate the overlaping period between the consumption period and the child period
            # we however assume that there os only one overlapping period between the two periods
            ###
            F.greatest(
                F.col("child_period_start"),
                F.col("parent_period_start"),
            ).alias("parent_child_overlap_period_start"),
            F.least(
                F.coalesce(
                    F.col("child_period_end"),
                    begining_of_year(F.current_date(), years_to_add=1),
                ),
                F.coalesce(
                    F.col("parent_period_end"),
                    begining_of_year(F.current_date(), years_to_add=1),
                ),
            ).alias("parent_child_overlap_period_end"),
        ).where(
            F.col("parent_child_overlap_period_start")
            < F.col("parent_child_overlap_period_end")
        )
        ###
    )


def handle_null_in_to_date_columns(df: DataFrame) -> DataFrame:
    return df.select(
        F.coalesce(
            F.col("parent_period_end"),
            begining_of_year(F.current_date(), years_to_add=1),
        ).alias("parent_period_end"),
        F.coalesce(
            F.col("child_period_end"),
            begining_of_year(F.current_date(), years_to_add=1),
        ).alias("child_period_end"),
        F.col("child_period_start"),
        F.col("parent_period_start"),
        F.col("child_metering_point_id"),
        F.col("parent_metering_point_id"),
    )


def join_child_to_parent_metering_point(
    consumption_metering_point_periods, child_metering_point_periods
):
    return (
        child_metering_point_periods.alias("child")
        .join(
            consumption_metering_point_periods.alias("parent"),
            F.col("child.parent_metering_point_id")
            == F.col("parent.metering_point_id"),
            "inner",
        )
        .where(
            (
                F.col("child.metering_point_type")
                == ELECTRICAL_HEATING_METERING_POINT_TYPE
            )
        )
        .select(
            F.col("child.metering_point_id").alias("child_metering_point_id"),
            F.col("child.parent_metering_point_id").alias("parent_metering_point_id"),
            F.col("parent.period_from_date").alias("parent_period_start"),
            F.col("parent.period_to_date").alias("parent_period_end"),
            F.col("child.coupled_date").alias("child_period_start"),
            F.col("child.uncoupled_date").alias("child_period_end"),
        )
    )
