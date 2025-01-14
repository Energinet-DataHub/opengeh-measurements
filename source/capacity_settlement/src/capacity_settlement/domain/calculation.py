from datetime import datetime
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, SparkSession, functions as F, Window

from telemetry_logging import use_span

from source.capacity_settlement.src.capacity_settlement.application.job_args.capacity_settlement_args import (
    CapacitySettlementArgs,
)


class ColumNames:
    date = "date"
    metering_point_id = "metering_point_id"
    observation_time = "observation_time"
    quantity = "quantity"
    selection_period_start = "selection_period_start"
    selection_period_end = "selection_period_end"


@use_span()
def execute(spark: SparkSession, args: CapacitySettlementArgs) -> None:
    # TODO JMG: read data from repository and call the `execute_core_logic` method
    pass


# This is also the function that will be tested using the `testcommon.etl` framework.
@use_span()
def execute_core_logic(
    time_series_points: DataFrame,
    metering_point_periods: DataFrame,
    calculation_month_start: datetime,
    calculation_month_end: datetime,
    time_zone: str,
) -> DataFrame:
    metering_point_periods = _add_selection_period_columns(
        metering_point_periods, calculation_month_end, time_zone
    )

    times_series_points = _average_ten_largest_quantities_in_selection_periods(
        time_series_points, metering_point_periods
    )

    times_series_points = _explode_to_daily(
        times_series_points,
        calculation_month_start,
        calculation_month_end,
        time_zone,
    )

    return times_series_points.select(
        ColumNames.metering_point_id, ColumNames.date, ColumNames.quantity
    )


def _add_selection_period_columns(
    metering_point_periods: DataFrame,
    calculation_month_end: datetime,
    time_zone: str,
) -> DataFrame:
    """
    Adds the selection period columns to the metering point periods DataFrame.
    The selection period is the period used to calculate the average of the ten largest quantities.
    The selection period is the last year up to the end of the calculation month.
    TODO: JMG: Should also support shorter metering point periods
    """

    # Convert to local time zone to ensure correct handling of leap year
    time_zone_info = ZoneInfo(time_zone)
    calculation_month_end_local = calculation_month_end.astimezone(time_zone_info)
    selection_period_start = (
        calculation_month_end_local - relativedelta(years=1)
    ).astimezone(ZoneInfo("UTC"))

    selection_period_end = calculation_month_end

    metering_point_periods = metering_point_periods.withColumn(
        ColumNames.selection_period_start, F.lit(selection_period_start)
    ).withColumn(ColumNames.selection_period_end, F.lit(selection_period_end))

    return metering_point_periods


def _average_ten_largest_quantities_in_selection_periods(
    time_series_points: DataFrame, metering_point_periods: DataFrame
) -> DataFrame:
    time_series_points = time_series_points.join(
        metering_point_periods, on=ColumNames.metering_point_id, how="inner"
    ).where(
        (F.col(ColumNames.observation_time) >= F.col(ColumNames.selection_period_start))
        & (F.col(ColumNames.observation_time) < F.col(ColumNames.selection_period_end))
    )

    grouping = [
        ColumNames.metering_point_id,
        ColumNames.selection_period_start,
        ColumNames.selection_period_end,
    ]

    window_spec = Window.partitionBy(grouping).orderBy(
        F.col(ColumNames.quantity).desc()
    )

    time_series_points = time_series_points.withColumn(
        "row_number", F.row_number().over(window_spec)
    ).filter(F.col("row_number") <= 10)

    measurements = time_series_points.groupBy(grouping).agg(
        F.avg(ColumNames.quantity).alias(ColumNames.quantity)
    )
    return measurements


def _explode_to_daily(
    df: DataFrame,
    calculation_month_start: datetime,
    calculation_month_end: datetime,
    time_zone: str,
) -> DataFrame:

    df = df.withColumn(
        "calculation_month_start",
        F.lit(calculation_month_start),
    ).withColumn(
        "calculation_month_end",
        F.lit(calculation_month_end),
    )

    return df.withColumn(
        "date_local_time",
        F.explode(
            F.sequence(
                F.from_utc_timestamp("calculation_month_start", time_zone),
                F.date_sub(F.from_utc_timestamp("calculation_month_end", time_zone), 1),
                F.expr("interval 1 day"),
            )
        ),
    ).withColumn(ColumNames.date, F.to_utc_timestamp("date_local_time", time_zone))
