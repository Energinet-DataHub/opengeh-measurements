from pyspark.sql import functions as F, types as T
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from telemetry_logging import use_span

import electrical_heating.infrastructure.electricity_market as em
import electrical_heating.infrastructure.measurements_gold as mg
from electrical_heating.application.job_args.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from electrical_heating.domain.constants import (
    ELECTRICAL_HEATING_LIMIT_YEARLY,
    ELECTRICAL_HEATING_METERING_POINT_TYPE,
)
from electrical_heating.domain.pyspark_functions import (
    convert_timezone,
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
    consumption_metering_point_periods = convert_timezone(
        consumption_metering_point_periods, time_zone, to_utc=False
    )
    child_metering_points = convert_timezone(
        child_metering_points, time_zone, to_utc=False
    )
    time_series_points = convert_timezone(time_series_points, time_zone, to_utc=False)

    metering_points_and_periods = (
        child_metering_points.alias("child")
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
            F.col("child.parent_metering_point_id").alias(
                "consumption_metering_point_id"
            ),
            # here we calculate the overlaping period between the consumption period and the child period
            # we however assume that there os only one overlapping period between the two periods
            F.greatest(
                F.col("child.coupled_date"),
                F.col("parent.period_from_date"),
            ).alias("parent_child_overlap_period_start"),
            F.least(
                F.coalesce(
                    F.col("child.uncoupled_date"),
                    begining_of_year(F.current_date(), years_to_add=1),
                ),
                F.coalesce(
                    F.col("parent.period_to_date"),
                    begining_of_year(F.current_date(), years_to_add=1),
                ),
            ).alias("parent_child_overlap_period_end"),
            # create a row for each year in the consumption period
            F.explode(
                F.sequence(
                    begining_of_year(F.col("parent.period_from_date")),
                    F.coalesce(
                        begining_of_year(F.col("parent.period_to_date")),
                        begining_of_year(F.current_date(), years_to_add=1),
                    ),
                    F.expr("INTERVAL 1 YEAR"),
                )
            ).alias("period_year"),
            F.col("parent.period_from_date").alias("consumption_period_start"),
            F.coalesce(
                F.col("parent.period_to_date"),
                begining_of_year(F.current_date(), years_to_add=1),
            ).alias("consumption_period_end"),
            F.col("child.coupled_date").alias("child_period_start"),
            F.coalesce(
                F.col("child.uncoupled_date"),
                begining_of_year(F.current_date(), years_to_add=1),
            ).alias("child_period_end"),
        )
        .where(
            F.col("parent_child_overlap_period_start")
            < F.col("parent_child_overlap_period_end")
        )
    )

    quarter_hour_consumption_periods_per_year = (
        metering_points_and_periods.alias("period")
        .join(
            time_series_points.alias("consumption"),
            F.col("period.consumption_metering_point_id")
            == F.col("consumption.metering_point_id"),
            "inner",
        )
        .where(
            (
                F.col("consumption.observation_time")
                >= F.col("period.parent_child_overlap_period_start")
            )
            & (
                F.col("consumption.observation_time")
                < F.col("period.parent_child_overlap_period_end")
            )
            & (
                F.year(F.col("consumption.observation_time"))
                == F.year(F.col("period.period_year"))
            )
        )
        .select(
            F.col("period.child_metering_point_id").alias("metering_point_id"),
            F.date_trunc("day", F.col("consumption.observation_time")).alias("date"),
            F.when(
                F.year(F.col("period.consumption_period_start"))
                == F.year(F.col("period_year")),
                F.col("period.consumption_period_start"),
            )
            .otherwise(begining_of_year(date=F.col("period_year")))
            .alias("consumption_period_start"),
            F.when(
                F.year(F.col("period.consumption_period_end"))
                == F.year(F.col("period_year")),
                F.col("period.consumption_period_end"),
            )
            .otherwise(begining_of_year(date=F.col("period_year"), years_to_add=1))
            .alias("consumption_period_end"),
            F.col("quantity"),
            F.col("observation_time"),
        )
    )

    # for aggreating comsumption from every 15 minutes to daily
    daily_window = Window.partitionBy(
        F.col("metering_point_id"),
        F.date_trunc("day", F.col("observation_time")),
    )

    daily_consumption_with_consumption_limit = (
        quarter_hour_consumption_periods_per_year.select(
            F.col("metering_point_id"),
            F.col("date"),
            F.sum(F.col("quantity")).over(daily_window).alias("quantity"),
            F.col("consumption_period_start"),
            F.col("consumption_period_end"),
            (
                F.datediff(
                    F.col("consumption_period_end"), F.col("consumption_period_start")
                )
                * ELECTRICAL_HEATING_LIMIT_YEARLY
                / days_in_year(F.col("consumption_period_start"))
            ).alias("period_consumption_limit"),
        ).drop_duplicates()
    )

    period_window = (
        Window.partitionBy(
            F.col("metering_point_id"),
            F.col("consumption_period_start"),
            F.col("consumption_period_end"),
        )
        .orderBy(F.col("date"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    period_consumption = (
        daily_consumption_with_consumption_limit.alias("daily")
        .select(
            F.col("metering_point_id"),
            F.col("date"),
            F.sum(F.col("quantity")).over(period_window).alias("cumulative_quantity"),
            F.col("quantity"),
            F.col("period_consumption_limit"),
        )
        .drop_duplicates()
    )

    period_consumption_with_limit = period_consumption.select(
        F.col("metering_point_id"),
        F.col("date"),
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
    ).drop_duplicates()

    period_consumption_with_limit = convert_timezone(
        period_consumption_with_limit, time_zone, to_utc=True
    )

    return period_consumption_with_limit
