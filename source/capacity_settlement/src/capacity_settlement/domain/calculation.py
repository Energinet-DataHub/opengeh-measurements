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
    metering_point_periods = metering_point_periods.withColumn(
        "selection_period_start", F.add_months(F.lit(calculation_period_end), -12)
    ).withColumn("selection_period_end", F.lit(calculation_period_end))

    metering_point_time_series = time_series_points.join(
        metering_point_periods, on="metering_point_id", how="inner"
    ).where(
        (F.col("observation_time") >= F.col("selection_period_start"))
        & (F.col("observation_time") < F.col("selection_period_end"))
    )

    grouping = ["metering_point_id", "selection_period_start", "selection_period_end"]

    window_spec = Window.partitionBy(grouping).orderBy(F.col("quantity").desc())

    metering_point_time_series = metering_point_time_series.withColumn(
        "row_number", F.row_number().over(window_spec)
    ).filter(F.col("row_number") <= 10)

    # Calculate the average of the top 10 quantities
    metering_point_time_series = metering_point_time_series.groupBy(grouping).agg(
        F.avg("quantity").alias("quantity")
    )

    metering_point_time_series = _explode_to_daily(
        metering_point_time_series,
        calculation_period_start,
        calculation_period_end,
        time_zone,
    )

    return metering_point_time_series.select("metering_point_id", "date", "quantity")


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
