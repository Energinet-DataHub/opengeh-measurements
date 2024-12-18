from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from electrical_heating.domain.pyspark_functions import convert_utc_to_localtime
import electrical_heating.infrastructure.measurements_gold as mg
import electrical_heating.infrastructure.electricity_market as em
from electrical_heating.entry_points.job_args.electrical_heating_args import (
    ElectricalHeatingArgs,
)


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
    _execute(
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        args.time_zone,
    )


# This is a temporary implementation. The final implementation will be provided in later PRs.
# This is also the function that will be tested using the `testcommon.etl` framework.
def _execute(
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

    df = (
        child_metering_point_periods.alias("child")
        .join(
            consumption_metering_point_periods.alias("periods"),
            F.col("child.parent_metering_point_id")
            == F.col("periods.metering_point_id"),
        )
        .join(
            time_series_points.alias("consumption"),
            F.col("child.parent_metering_point_id")
            == F.col("consumption.metering_point_id"),
        )
        .groupBy(
            F.col("child.metering_point_id"),
            F.date_trunc("day", "observation_time").alias("date"),
        )
        .agg(F.sum("quantity").alias("quantity"))
        .select(
            F.col("child.metering_point_id"),
            F.col("date"),
            F.col("quantity"),
        )
    )

    return df
