from datetime import datetime
from uuid import UUID
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta
from geh_common.telemetry import use_span
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from geh_calculated_measurements.capacity_settlement.domain.calculated_names import CalculatedNames
from geh_calculated_measurements.capacity_settlement.domain.calculation_output import (
    CalculationOutput,
)
from geh_calculated_measurements.capacity_settlement.domain.column_names import ColumNames


# This is also the function that will be tested using the `testcommon.etl` framework.
@use_span()
def execute_core_logic(
    spark: SparkSession,
    time_series_points: DataFrame,
    metering_point_periods: DataFrame,
    orchestration_instance_id: UUID,
    calculation_month: int,
    calculation_year: int,
    time_zone: str,
    execution_time: str,
) -> CalculationOutput:
    calculations = _create_calculations(
        spark,
        orchestration_instance_id,
        calculation_month,
        calculation_year,
        execution_time,
    )

    metering_point_periods = _add_selection_period_columns(
        metering_point_periods,
        calculation_month=calculation_month,
        calculation_year=calculation_year,
        time_zone=time_zone,
    )

    time_series_points_hourly = _transform_quarterly_time_series_to_hourly(time_series_points)

    grouping = [
        ColumNames.metering_point_id,
        ColumNames.selection_period_start,
        ColumNames.selection_period_end,
        ColumNames.child_metering_point_id,
        ColumNames.child_period_from_date,
        ColumNames.child_period_to_date,
    ]

    time_series_points_ten_largest_quantities = _ten_largest_quantities_in_selection_periods(
        time_series_points_hourly, metering_point_periods, grouping
    )

    ten_largest_quantities = time_series_points_ten_largest_quantities.select(
        ColumNames.metering_point_id,
        ColumNames.quantity,
        CalculatedNames.observation_time_hourly,
    )

    time_series_points_average_ten_largest_quantities = _average_ten_largest_quantities_in_selection_periods(
        time_series_points_ten_largest_quantities, grouping
    )

    time_series_points_exploded_to_daily = _explode_to_daily(
        time_series_points_average_ten_largest_quantities,
        calculation_month,
        calculation_year,
        time_zone,
    )

    time_series_points_within_child_period = _filter_date_within_child_period(time_series_points_exploded_to_daily)

    measurements = time_series_points_within_child_period.select(
        F.col(ColumNames.child_metering_point_id).alias(ColumNames.metering_point_id),
        F.col(ColumNames.date),
        F.col(ColumNames.quantity).cast(DecimalType(18, 3)),
    )

    ten_largest_quantities = ten_largest_quantities.withColumnRenamed(
        CalculatedNames.observation_time_hourly, ColumNames.observation_time
    )

    calculation_output = CalculationOutput(
        measurements=measurements,
        calculations=calculations,
        ten_largest_quantities=ten_largest_quantities,
    )

    return calculation_output


def _create_calculations(
    spark: SparkSession,
    orchestration_instance_id: UUID,
    calculation_month: int,
    calculation_year: int,
    execution_time: str,
) -> DataFrame:
    return spark.createDataFrame(
        [
            (
                str(orchestration_instance_id),
                calculation_year,
                calculation_month,
                execution_time,
            )
        ],
        schema="orchestration_instance_id STRING, year INT, month INT, execution_time STRING",
    )


def _transform_quarterly_time_series_to_hourly(
    time_series_points: DataFrame,
) -> DataFrame:
    # Reduces observation time to hour value
    time_series_points = time_series_points.select(
        "*", F.date_trunc("hour", ColumNames.observation_time).alias(CalculatedNames.observation_time_hourly)
    ).drop(ColumNames.observation_time)

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
    """
    calculation_start_date = datetime(calculation_year, calculation_month, 1, tzinfo=ZoneInfo(time_zone))
    calculation_end_date = calculation_start_date + relativedelta(months=1)

    selection_period_start_min = calculation_end_date - relativedelta(years=1)
    selection_period_end_max = calculation_end_date

    metering_point_periods = metering_point_periods.filter(
        (F.col(CalculatedNames.period_from_date) < calculation_end_date)
        & (
            (F.col(CalculatedNames.period_to_date) > calculation_start_date)
            | F.col(CalculatedNames.period_to_date).isNull()
        )
    )

    return metering_point_periods.select(
        "*",
        F.when(
            F.col(CalculatedNames.period_from_date) > F.lit(selection_period_start_min),
            F.col(CalculatedNames.period_from_date),
        )
        .otherwise(F.lit(selection_period_start_min))
        .alias(ColumNames.selection_period_start),
        F.when(
            F.col(CalculatedNames.period_to_date) <= F.lit(selection_period_end_max),
            F.col(CalculatedNames.period_to_date),
        )
        .otherwise(F.lit(selection_period_end_max))
        .alias(ColumNames.selection_period_end),
    )


def _ten_largest_quantities_in_selection_periods(
    time_series_points: DataFrame,
    metering_point_periods: DataFrame,
    grouping: list[str],
) -> DataFrame:
    time_series_points = time_series_points.join(
        metering_point_periods, on=ColumNames.metering_point_id, how="inner"
    ).where(
        (F.col(CalculatedNames.observation_time_hourly) >= F.col(ColumNames.selection_period_start))
        & (F.col(CalculatedNames.observation_time_hourly) < F.col(ColumNames.selection_period_end))
    )

    window_spec = Window.partitionBy(grouping).orderBy(F.col(ColumNames.quantity).desc())

    time_series_points = (
        time_series_points.select("*", F.row_number().over(window_spec).alias("row_number"))
        .filter(F.col("row_number") <= 10)
        .drop("row_number")
    )

    return time_series_points


def _average_ten_largest_quantities_in_selection_periods(
    time_series_points: DataFrame, grouping: list[str]
) -> DataFrame:
    return time_series_points.groupBy(grouping).agg(F.avg(ColumNames.quantity).alias(ColumNames.quantity))


def _explode_to_daily(
    df: DataFrame,
    calculation_month: int,
    calculation_year: int,
    time_zone: str,
) -> DataFrame:
    calculation_start_date = datetime(calculation_year, calculation_month, 1, tzinfo=ZoneInfo(time_zone))
    calculation_end_date = calculation_start_date + relativedelta(months=1) - relativedelta(days=1)

    return (
        df.select(
            "*",
            F.explode(
                F.sequence(
                    F.lit(calculation_start_date.date()), F.lit(calculation_end_date.date()), F.expr("interval 1 day")
                )
            ).alias("date_local"),
        )
        .select("*", F.to_utc_timestamp("date_local", time_zone).alias(ColumNames.date))
        .filter(
            (F.col(ColumNames.selection_period_start) <= F.col(ColumNames.date))
            & (F.col(ColumNames.selection_period_end) > F.col(ColumNames.date))
        )
    )


def _filter_date_within_child_period(time_series_points_exploded_to_daily: DataFrame) -> DataFrame:
    return time_series_points_exploded_to_daily.filter(
        (F.col(ColumNames.date) >= F.col(ColumNames.child_period_from_date))
        & (
            (F.col(ColumNames.date) < F.col(ColumNames.child_period_to_date))
            | F.col(ColumNames.child_period_to_date).isNull()
        )
    )
