import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from telemetry_logging import use_span

import source.electrical_heating.src.electrical_heating.infrastructure.electricity_market as em
import source.electrical_heating.src.electrical_heating.infrastructure.measurements_gold as mg
from source.electrical_heating.src.electrical_heating.application.entry_points.job_args.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from source.electrical_heating.src.electrical_heating.domain.constants import (
    ELECTRICAL_HEATING_LIMIT_YEARLY,
    ELECTRICAL_HEATING_METERING_POINT_TYPE,
)
from source.electrical_heating.src.electrical_heating.domain.pyspark_functions import (
    convert_utc_to_localtime,
    convert_localtime_to_utc,
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
    child_metering_point_periods = (
        electricity_market_repository.read_child_metering_point_periods()
    )
    time_series_points = measurements_gold_repository.read_time_series_points()

    # Execute the calculation logic
    execute_core_logic(
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        args.time_zone,
    )


# This is a temporary implementation. The final implementation will be provided in later PRs.
# This is also the function that will be tested using the `testcommon.etl` framework.
@use_span()
def execute_core_logic(
    time_series_points: DataFrame,
    consumption_metering_point_periods: DataFrame,
    child_metering_point_periods: DataFrame,
    time_zone: str,
) -> DataFrame:
    time_series_points = convert_utc_to_localtime(
        time_series_points, mg.ColumnNames.observation_time, time_zone
    )

    consumption_metering_point_periods = convert_utc_to_localtime(
        consumption_metering_point_periods, em.ColumnNames.period_from_date, time_zone
    )
    consumption_metering_point_periods = convert_utc_to_localtime(
        consumption_metering_point_periods, em.ColumnNames.period_to_date, time_zone
    )

    child_metering_point_periods = child_metering_point_periods.where(
        F.col(em.ColumnNames.metering_point_type)
        == em.MeteringPointType.ELECTRICAL_HEATING.value
    )
    child_metering_point_periods = convert_utc_to_localtime(
        child_metering_point_periods, em.ColumnNames.period_from_date, time_zone
    )
    child_metering_point_periods = convert_utc_to_localtime(
        child_metering_point_periods, em.ColumnNames.period_to_date, time_zone
    )

    metering_points_and_periods = (
        child_metering_point_periods.alias("child")
        .join(
            consumption_metering_point_periods.alias("metering"),
            F.col("child.parent_metering_point_id")
            == F.col("metering.metering_point_id"),
            "inner",
        )
        .select(
            F.col("child.metering_point_id").alias("child_metering_point_id"),
            F.col("child.parent_metering_point_id").alias(
                "consumption_metering_point_id"
            ),
            F.greatest(
                F.col("child.period_from_date"),
                F.col("metering.period_from_date"),
            ).alias("parent_child_overlap_period_start"),
            F.least(
                F.coalesce(
                    F.col("child.period_to_date"),
                    F.concat(
                        (F.year(F.current_date()) + 1).cast("string"),
                        F.lit("-01-01 00:00:00"),
                    ).cast("timestamp"),
                ),
                F.coalesce(
                    F.col("metering.period_to_date"),
                    F.concat(
                        (F.year(F.current_date()) + 1).cast("string"),
                        F.lit("-01-01 00:00:00"),
                    ).cast("timestamp"),
                ),
            ).alias("parent_child_overlap_period_end"),
            F.explode(
                F.sequence(
                    F.year(F.col("metering.period_from_date")),
                    F.year(
                        F.coalesce(
                            F.col("metering.period_to_date"),
                            F.concat(
                                (F.year(F.current_date()) + 1).cast("string"),
                                F.lit("-01-01 00:00:00"),
                            ).cast("timestamp"),
                        )
                    ),
                )
            ).alias("period_year"),
            F.col("metering.period_from_date").alias("consumption_period_start"),
            F.coalesce(
                F.col("metering.period_to_date"),
                F.concat(
                    (F.year(F.current_date()) + 1).cast("string"),
                    F.lit("-01-01 00:00:00"),
                ).cast("timestamp"),
            ).alias("consumption_period_end"),
            F.col("child.period_from_date").alias("child_period_start"),
            F.coalesce(
                F.col("child.period_to_date"),
                F.concat(
                    (F.year(F.current_date()) + 1).cast("string"),
                    F.lit("-01-01 00:00:00"),
                ).cast("timestamp"),
            ).alias("child_period_end"),
        )
        .where(
            F.col("parent_child_overlap_period_start")
            < F.col("parent_child_overlap_period_end")
        )
        .where((F.col("child.metering_point_type") == ELECTRICAL_HEATING_TYPE))
    )

    print("--- metering_points_and_periods_df ---")
    metering_points_and_periods.show()

    daily_window = Window.partitionBy(
        F.col("consumption.metering_point_id"),
        F.date_trunc("day", F.col("consumption.observation_time")),
    )

    daily_child_consumption = (
        metering_points_and_periods.alias("period")
        .join(
            time_series_points.alias("consumption"),
            F.col("period.consumption_metering_point_id")
            == F.col("consumption.metering_point_id"),
            "inner",
        )
        .filter(
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
                == F.col("period.period_year")
            )
        )
        .select(
            F.col("period.child_metering_point_id").alias("metering_point_id"),
            F.date_trunc("day", F.col("consumption.observation_time")).alias("date"),
            F.sum(F.col("consumption.quantity")).over(daily_window).alias("quantity"),
            F.when(
                F.year(F.col("period.consumption_period_start"))
                == F.col("period_year"),
                F.col("period.consumption_period_start"),
            )
            .otherwise(
                F.concat(
                    F.col("period_year").cast("string"), F.lit("-01-01 00:00:00")
                ).cast("timestamp")
            )
            .alias("consumption_period_start"),
            F.when(
                F.year(F.col("period.consumption_period_end")) == F.col("period_year"),
                F.col("period.consumption_period_end"),
            )
            .otherwise(
                F.concat(
                    (F.col("period_year") + 1).cast("string"), F.lit("-01-01 00:00:00")
                ).cast("timestamp")
            )
            .alias("consumption_period_end"),
        )
        .drop_duplicates()
    )

    print("--- daily_child_consumption_df ---")
    daily_child_consumption.show()

    daily_consumption_with_consumption_limit = daily_child_consumption.select(
        F.col("metering_point_id"),
        F.col("date"),
        F.col("quantity"),
        F.col("consumption_period_start"),
        F.col("consumption_period_end"),
        (
            F.datediff(
                F.col("consumption_period_end"), F.col("consumption_period_start")
            )
            * ELECTRICAL_HEATING_LIMIT_YEARLY
            / F.dayofyear(
                F.last_day(
                    F.concat(
                        F.year(F.col("consumption_period_start")).cast("string"),
                        F.lit("-12-31"),
                    ).cast("timestamp")
                )
            )
        ).alias("period_consumption_limit"),
    )

    print("--- daily_consumption_with_consumption_limit_df ---")
    daily_consumption_with_consumption_limit.show()

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
    print("--- period_consumption_df ---")
    period_consumption.show()

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
        .alias("quantity"),
    ).drop_duplicates()

    period_consumption_with_limit = convert_localtime_to_utc(
        period_consumption_with_limit, "date", time_zone
    )

    return period_consumption_with_limit
