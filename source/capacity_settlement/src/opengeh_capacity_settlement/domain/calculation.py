from datetime import datetime
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, SparkSession, functions as F, Window

from telemetry_logging import use_span

from opengeh_capacity_settlement.application.job_args.capacity_settlement_args import (
    CapacitySettlementArgs,
)


class ColumNames:
    # Input/output column names from contracts
    date = "date"
    metering_point_id = "metering_point_id"
    observation_time = "observation_time"
    quantity = "quantity"
    # Ephemeral columns
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
    calculation_month: int,
    calculation_year: int,
    time_zone: str,
) -> DataFrame:
    metering_point_periods = _add_selection_period_columns(
        metering_point_periods,
        calculation_month=calculation_month,
        calculation_year=calculation_year,
        time_zone=time_zone,
    )

    times_series_points = _average_ten_largest_quantities_in_selection_periods(
        time_series_points, metering_point_periods
    )

    times_series_points = _explode_to_daily(times_series_points, calculation_month, calculation_year, time_zone)

    return times_series_points.select(ColumNames.metering_point_id, ColumNames.date, ColumNames.quantity)


def _add_selection_period_columns(
    metering_point_periods: DataFrame,
    calculation_month: int,
    calculation_year: int,
    time_zone: str,
) -> DataFrame:
    """
    Adds the selection period columns to the metering point periods DataFrame.
    The selection period is the period used to calculate the average of the ten largest quantities.
    The selection period is the last year up to the end of the calculation month.
    TODO: JMG: Should also support shorter metering point periods
    """

    calculation_start_date = datetime(calculation_year, calculation_month, 1, tzinfo=ZoneInfo(time_zone))
    calculation_end_date = calculation_start_date + relativedelta(months=1)

    selection_period_start = calculation_end_date - relativedelta(years=1)
    selection_period_end = calculation_end_date

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

    window_spec = Window.partitionBy(grouping).orderBy(F.col(ColumNames.quantity).desc())

    time_series_points = time_series_points.withColumn("row_number", F.row_number().over(window_spec)).filter(
        F.col("row_number") <= 10
    )

    measurements = time_series_points.groupBy(grouping).agg(F.avg(ColumNames.quantity).alias(ColumNames.quantity))
    return measurements


def _explode_to_daily(
    df: DataFrame,
    calculation_month: int,
    calculation_year: int,
    time_zone: str,
) -> DataFrame:
    calculation_start_date = datetime(calculation_year, calculation_month, 1, tzinfo=ZoneInfo(time_zone))
    calculation_end_date = calculation_start_date + relativedelta(months=1) - relativedelta(days=1)

    df = df.withColumn(
        "date_local",
        F.explode(
            F.sequence(
                F.lit(calculation_start_date.date()),
                F.lit(calculation_end_date.date()),
                F.expr("interval 1 day"),
            )
        ),
    )

    df = df.withColumn(ColumNames.date, F.to_utc_timestamp("date_local", time_zone))

    return df
