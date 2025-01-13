from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, functions as F, Window

from telemetry_logging import use_span

from source.capacity_settlement.src.capacity_settlement.application.job_args.capacity_settlement_args import (
    CapacitySettlementArgs,
)


@use_span()
def execute(spark: SparkSession, args: CapacitySettlementArgs) -> None:
    # TODO JMG: read data from repository and call the `execute_core_logic` method
    pass


# This is also the function that will be tested using the `testcommon.etl` framework.
@use_span()
def execute_core_logic(
    time_series_points: DataFrame,
    metering_point_periods: DataFrame,
    calculation_period_start: datetime,
    calculation_period_end: datetime,
    time_zone: str,
) -> DataFrame:

    times_series_points = _filter_on_selection_period(
        time_series_points, metering_point_periods, calculation_period_end
    )

    times_series_points = _average_ten_largest_quantities(times_series_points)

    times_series_points = _explode_to_daily(
        times_series_points,
        calculation_period_start,
        calculation_period_end,
        time_zone,
    )

    return times_series_points.select("metering_point_id", "date", "quantity")


def _filter_on_selection_period(
    time_series_points: DataFrame,
    metering_point_periods: DataFrame,
    calculation_period_end: datetime,
) -> DataFrame:
    metering_point_periods = metering_point_periods.withColumn(
        "selection_period_start", F.add_months(F.lit(calculation_period_end), -12)
    ).withColumn("selection_period_end", F.lit(calculation_period_end))

    return time_series_points.join(
        metering_point_periods, on="metering_point_id", how="inner"
    ).where(
        (F.col("observation_time") >= F.col("selection_period_start"))
        & (F.col("observation_time") < F.col("selection_period_end"))
    )


def _average_ten_largest_quantities(
    df: DataFrame,
) -> DataFrame:
    grouping = ["metering_point_id", "selection_period_start", "selection_period_end"]

    window_spec = Window.partitionBy(grouping).orderBy(F.col("quantity").desc())

    df = df.withColumn("row_number", F.row_number().over(window_spec)).filter(
        F.col("row_number") <= 10
    )

    df = df.groupBy(grouping).agg(F.avg("quantity").alias("quantity"))
    return df


def _explode_to_daily(
    df: DataFrame,
    calculation_period_start: datetime,
    calculation_period_end: datetime,
    time_zone: str,
) -> DataFrame:

    df = df.withColumn(
        "calculation_period_start",
        F.lit(calculation_period_start),
    ).withColumn(
        "calculation_period_end",
        F.lit(calculation_period_end),
    )

    return df.withColumn(
        "date_local_time",
        F.explode(
            F.sequence(
                F.from_utc_timestamp("calculation_period_start", time_zone),
                F.date_sub(
                    F.from_utc_timestamp("calculation_period_end", time_zone), 1
                ),
                F.expr("interval 1 day"),
            )
        ),
    ).withColumn("date", F.to_utc_timestamp("date_local_time", time_zone))
