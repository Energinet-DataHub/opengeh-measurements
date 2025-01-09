import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window
from telemetry_logging import use_span

import source.electrical_heating.src.electrical_heating.infrastructure.electricity_market as em
import source.electrical_heating.src.electrical_heating.infrastructure.measurements_gold as mg
from source.electrical_heating.src.electrical_heating.domain.constants import (
    ELECTRICAL_HEATING_LIMIT,
)
from source.electrical_heating.src.electrical_heating.domain.pyspark_functions import (
    convert_utc_to_localtime,
    convert_localtime_to_utc,
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

    metering_id_and_date_window = Window.partitionBy(
        "metering_point_id", F.year("date")
    ).orderBy(F.unix_timestamp("date").cast("long"))

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
            F.sum(F.col("quantity"))
            .over(metering_id_and_date_window)
            .alias("cumulative_quantity"),
        )
    )

    daily_child_consumption_with_limit = daily_child_consumption.select(
        F.col("metering_point_id"),
        F.col("date"),
        F.when(
            F.col("cumulative_quantity") > ELECTRICAL_HEATING_LIMIT,
            0,
        )
        .when(
            F.col("cumulative_quantity")
            >= ELECTRICAL_HEATING_LIMIT + F.col("quantity"),
            ELECTRICAL_HEATING_LIMIT - F.col("cumulative_quantity"),
        )
        .otherwise(
            F.col("quantity"),
        )
        .alias("quantity"),
    )

    daily_child_consumption_with_limit = convert_localtime_to_utc(
        daily_child_consumption_with_limit, "date", time_zone
    )

    return daily_child_consumption_with_limit
