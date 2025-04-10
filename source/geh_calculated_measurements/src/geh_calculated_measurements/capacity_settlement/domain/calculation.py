from datetime import datetime
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from geh_calculated_measurements.capacity_settlement.domain import TenLargestQuantities
from geh_calculated_measurements.capacity_settlement.domain.calculation_output import (
    CalculationOutput,
)
from geh_calculated_measurements.capacity_settlement.domain.ephemeral_column_names import EphemeralColumnNames
from geh_calculated_measurements.capacity_settlement.domain.model.metering_point_periods import MeteringPointPeriods
from geh_calculated_measurements.common.domain import (
    ContractColumnNames,
    CurrentMeasurements,
)
from geh_calculated_measurements.common.domain.model import CalculatedMeasurementsDaily


@use_span()
@testing(selector=lambda x: x.calculations)
@testing(selector=lambda x: x.calculated_measurements)
@testing(selector=lambda x: x.ten_largest_quantities)
def execute(
    current_measurements: CurrentMeasurements,
    metering_point_periods: MeteringPointPeriods,
    calculation_month: int,
    calculation_year: int,
    time_zone: str,
) -> CalculationOutput:
    metering_point_periods_with_selection_period = _add_selection_period_columns(
        metering_point_periods.df,
        calculation_month=calculation_month,
        calculation_year=calculation_year,
        time_zone=time_zone,
    )

    time_series_points_hourly = _transform_quarterly_time_series_to_hourly(current_measurements.df)

    grouping = [
        ContractColumnNames.metering_point_id,
        EphemeralColumnNames.selection_period_start,
        EphemeralColumnNames.selection_period_end,
        ContractColumnNames.child_metering_point_id,
        ContractColumnNames.child_period_from_date,
        ContractColumnNames.child_period_to_date,
    ]

    time_series_points_ten_largest_quantities = _ten_largest_quantities_in_selection_periods(
        time_series_points_hourly, metering_point_periods_with_selection_period, grouping
    )

    ten_largest_quantities = TenLargestQuantities(
        time_series_points_ten_largest_quantities.select(
            F.col(ContractColumnNames.metering_point_id),
            F.col(ContractColumnNames.quantity).cast(DecimalType(18, 3)),
            F.col(ContractColumnNames.observation_time),
        )
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
        F.col(ContractColumnNames.child_metering_point_id).alias(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.date),
        F.col(ContractColumnNames.quantity).cast(DecimalType(18, 3)),
    )

    calculation_output = CalculationOutput(
        calculated_measurements_daily=CalculatedMeasurementsDaily(measurements),
        ten_largest_quantities=ten_largest_quantities,
    )

    return calculation_output


@testing()
def _transform_quarterly_time_series_to_hourly(
    time_series_points: DataFrame,
) -> DataFrame:
    # Reduces observation time to hour value
    time_series_points = time_series_points.withColumn(
        ContractColumnNames.observation_time, F.date_trunc("hour", ContractColumnNames.observation_time)
    )
    # group by all columns except quantity and then sum the quantity
    group_by = [col for col in time_series_points.columns if col != ContractColumnNames.quantity]
    time_series_points = time_series_points.groupBy(group_by).agg(
        F.sum(ContractColumnNames.quantity).alias(ContractColumnNames.quantity)
    )

    return time_series_points


@testing()
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
        (F.col(ContractColumnNames.period_from_date) < calculation_end_date)
        & (
            (F.col(ContractColumnNames.period_to_date) > calculation_start_date)
            | F.col(ContractColumnNames.period_to_date).isNull()
        )
    )

    metering_point_periods = metering_point_periods.withColumn(
        EphemeralColumnNames.selection_period_start,
        F.when(
            F.col(ContractColumnNames.period_from_date) > F.lit(selection_period_start_min),
            F.col(ContractColumnNames.period_from_date),
        ).otherwise(F.lit(selection_period_start_min)),
    ).withColumn(
        EphemeralColumnNames.selection_period_end,
        F.when(
            F.col(ContractColumnNames.period_to_date) <= F.lit(selection_period_end_max),
            F.col(ContractColumnNames.period_to_date),
        ).otherwise(F.lit(selection_period_end_max)),
    )

    return metering_point_periods


@testing()
def _ten_largest_quantities_in_selection_periods(
    time_series_points: DataFrame,
    metering_point_periods: DataFrame,
    grouping: list[str],
) -> DataFrame:
    time_series_points = time_series_points.join(
        metering_point_periods, on=ContractColumnNames.metering_point_id, how="inner"
    ).where(
        (F.col(ContractColumnNames.observation_time) >= F.col(EphemeralColumnNames.selection_period_start))
        & (F.col(ContractColumnNames.observation_time) < F.col(EphemeralColumnNames.selection_period_end))
    )

    window_spec = Window.partitionBy(grouping).orderBy(F.col(ContractColumnNames.quantity).desc())

    time_series_points = time_series_points.withColumn("row_number", F.row_number().over(window_spec)).filter(
        F.col("row_number") <= 10
    )

    return time_series_points


@testing()
def _average_ten_largest_quantities_in_selection_periods(
    time_series_points: DataFrame, grouping: list[str]
) -> DataFrame:
    measurements = time_series_points.groupBy(grouping).agg(
        F.avg(ContractColumnNames.quantity).alias(ContractColumnNames.quantity)
    )
    return measurements


@testing()
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

    df = df.withColumn(ContractColumnNames.date, F.to_utc_timestamp("date_local", time_zone))

    df = df.filter(
        (F.col(EphemeralColumnNames.selection_period_start) <= F.col(ContractColumnNames.date))
        & (F.col(EphemeralColumnNames.selection_period_end) > F.col(ContractColumnNames.date))
    )

    return df


@testing()
def _filter_date_within_child_period(time_series_points_exploded_to_daily: DataFrame) -> DataFrame:
    return time_series_points_exploded_to_daily.filter(
        (F.col(ContractColumnNames.date) >= F.col(ContractColumnNames.child_period_from_date))
        & (
            (F.col(ContractColumnNames.date) < F.col(ContractColumnNames.child_period_to_date))
            | F.col(ContractColumnNames.child_period_to_date).isNull()
        )
    )
