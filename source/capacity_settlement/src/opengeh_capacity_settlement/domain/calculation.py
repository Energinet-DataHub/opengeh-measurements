from datetime import datetime
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from telemetry_logging import use_span

from opengeh_capacity_settlement.application.job_args.capacity_settlement_args import (
    CapacitySettlementArgs,
)
from opengeh_capacity_settlement.domain.calculation_output import CalculationOutput


class ColumNames:
    # Input/output column names from contracts
    date = "date"
    child_metering_point_id = "child_metering_point_id"
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
    spark: SparkSession,
    time_series_points: DataFrame,
    metering_point_periods: DataFrame,
    calculation_month: int,
    calculation_year: int,
    time_zone: str,
) -> CalculationOutput:
    calculation_output = CalculationOutput()

    metering_point_periods = _add_selection_period_columns(
        metering_point_periods,
        calculation_month=calculation_month,
        calculation_year=calculation_year,
        time_zone=time_zone,
    )

    time_series_points = _transform_quarterly_time_series_to_hourly(time_series_points)

    times_series_points = _average_ten_largest_quantities_in_selection_periods(
        time_series_points, metering_point_periods
    )

    times_series_points = _explode_to_daily(times_series_points, calculation_month, calculation_year, time_zone)
    times_series_points.show(1000)

    calculation_output.measurements = times_series_points.select(
        F.col(ColumNames.child_metering_point_id).alias(ColumNames.metering_point_id),
        F.col(ColumNames.date),
        F.col(ColumNames.quantity).cast(DecimalType(18, 3)),
    )

    calculation_output.calculations = spark.createDataFrame([], schema="")

    return calculation_output


def _transform_quarterly_time_series_to_hourly(time_series_points: DataFrame) -> DataFrame:
    # Reduces observation time to hour value
    time_series_points = time_series_points.withColumn(
        ColumNames.observation_time, F.date_trunc("hour", ColumNames.observation_time)
    )
    # group by all columns except quantity and then sum the quantity
    group_by = [col for col in time_series_points.columns if col != ColumNames.quantity]
    time_series_points = time_series_points.groupBy(group_by).agg(F.sum(ColumNames.quantity).alias(ColumNames.quantity))

    return time_series_points


def _add_selection_period_columns(
    metering_point_periods: DataFrame,
    calculation_month: int,
    calculation_year: int,
    time_zone: str,
) -> DataFrame:
    """Add the selection period columns to the metering point periods DataFrame.

    The selection period is the period used to calculate the average of the ten largest quantities.
    The selection period is the last year up to the end of the calculation month.
    TODO: JMG: Should also support shorter metering point periods.
    """
    calculation_start_date = datetime(calculation_year, calculation_month, 1, tzinfo=ZoneInfo(time_zone))
    calculation_end_date = calculation_start_date + relativedelta(months=1)

    selection_period_start = calculation_end_date - relativedelta(years=1)
    selection_period_end = calculation_end_date

    metering_point_periods.show()
    metering_point_periods = metering_point_periods.filter(
        (F.year(F.col("period_from_date")) < calculation_year) |
        ((F.year(F.col("period_from_date")) == calculation_year) &
         (F.month(F.col("period_from_date")) <= calculation_month))
    ).filter(
        (F.col("period_to_date").isNull()) |
        (F.year(F.col("period_to_date")) > calculation_year) |
        ((F.year(F.col("period_to_date")) == calculation_year) &
         (F.month(F.col("period_to_date")) >= calculation_month))
    )
    
    metering_point_periods.show()
    metering_point_periods = metering_point_periods.withColumn(
        ColumNames.selection_period_start,
        F.when(F.col("period_from_date") > F.lit(selection_period_start), F.col("period_from_date"))
        .otherwise(F.lit(selection_period_start))
    ).withColumn(
        ColumNames.selection_period_end,
        F.when(F.col("period_to_date") <= F.lit(selection_period_end), F.col("period_to_date"))
        .otherwise(F.lit(selection_period_end))
    )

    metering_point_periods.show()

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
        ColumNames.child_metering_point_id,
    ]

    window_spec = Window.partitionBy(grouping).orderBy(F.col(ColumNames.quantity).desc())

    time_series_points = time_series_points.withColumn("row_number", F.row_number().over(window_spec)).filter(
        F.col("row_number") <= 10
    )
    time_series_points.show()

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
