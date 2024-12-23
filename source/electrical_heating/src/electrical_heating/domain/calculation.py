from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from telemetry_logging import use_span

from source.electrical_heating.src.electrical_heating.entry_points.job_args.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from pyspark_functions import (
    convert_utc_to_localtime,
    convert_localtime_to_utc,
)
from electrical_heating import infrastructure as mg, infrastructure as em


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

    child_metering_point_periods = child_metering_point_periods.where(
        F.col(em.ColumnNames.metering_point_type)
        == em.MeteringPointType.ELECTRICAL_HEATING.value
    )
    child_metering_point_periods = convert_utc_to_localtime(
        child_metering_point_periods, em.ColumnNames.period_to_date, time_zone
    )

    overlapping_metering_and_child_periods = (
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
                F.col("child.period_from_date"), F.col("metering.period_from_date")
            ).alias("period_start"),
            F.least(
                F.col("child.period_to_date"), F.col("metering.period_to_date")
            ).alias("period_end"),
        )
        .where(F.col("period_start") < F.col("period_end"))
    )

    daily_child_consumption = (
        overlapping_metering_and_child_periods.alias("period")
        .join(
            time_series_points.alias("consumption"),
            F.col("period.consumption_metering_point_id")
            == F.col("consumption.metering_point_id"),
            "inner",
        )
        .filter(
            (F.col("consumption.observation_time") >= F.col("period.period_start"))
            & (F.col("consumption.observation_time") < F.col("period.period_end"))
        )
        .groupBy(
            F.date_trunc("day", "consumption.observation_time").alias("date"),
            F.col("period.child_metering_point_id").alias("metering_point_id"),
        )
        .agg(F.sum("consumption.quantity").alias("quantity"))
        .select(
            F.col("metering_point_id"),
            F.col("date"),
            F.col("quantity"),
        )
    )

    daily_child_consumption = convert_localtime_to_utc(
        daily_child_consumption, "date", time_zone
    )

    return daily_child_consumption
