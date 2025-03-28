from datetime import datetime

import pyspark.sql.functions as F
from geh_common.domain.types import MeteringPointType
from geh_common.pyspark.transformations import convert_from_utc, convert_to_utc
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import Column, DataFrame
from pyspark.sql import types as T

from geh_calculated_measurements.common.domain import (
    ContractColumnNames,
)
from geh_calculated_measurements.net_consumption_group_6.domain import (
    Cenc,
    TimeSeriesPoints,
)


def days_in_year(year: Column, month: Column) -> Column:
    # Create date for January 1st of the given year (for Column input)
    start_date = F.to_date(
        F.concat(
            year.cast(T.StringType()),
            F.lit("-"),
            month.cast(T.StringType()),
            F.lit("-01"),
        )
    )
    # Create date for January 1st of the next year
    end_date = F.to_date(
        F.concat((year + 1).cast(T.StringType()), F.lit("-"), month.cast(T.StringType()), F.lit("-01"))
    )

    # Calculate the difference in days
    return F.datediff(end_date, start_date)


@use_span()
@testing()
def calculate_daily(
    time_series_points: TimeSeriesPoints,
    cenc: Cenc,
    time_zone: str,
    execution_start_datetime: datetime,
) -> DataFrame:
    cenc_added_col = cenc.df.select(  # adding needed columns
        "*",
        F.lit(MeteringPointType.NET_CONSUMPTION.value).alias(ContractColumnNames.metering_point_type),
        F.lit(execution_start_datetime).alias("transaction_creation_datetime"),
    )

    time_series_points_df = time_series_points.df

    cenc_added_col = convert_from_utc(cenc_added_col, time_zone)

    time_series_points_df = convert_from_utc(time_series_points_df, time_zone)

    cenc_selected_col = cenc_added_col.select(  # selecting needed columns
        F.col("metering_point_id"),
        F.col("metering_point_type"),
        (F.col("quantity") / days_in_year(F.col("settlement_year"), F.col("settlement_month")))
        .cast(T.DecimalType(18, 3))
        .alias("quantity"),
        F.col("transaction_creation_datetime"),
    )

    latest_measurements_date = (
        time_series_points_df.where(F.col("metering_point_type") == MeteringPointType.NET_CONSUMPTION.value)
        .groupBy("metering_point_id")
        .agg(F.max("observation_time").alias("latest_observation_date"))
    )

    # merging the with internal
    cenc_w_last_run = (
        cenc_selected_col.alias("cenc")
        .join(
            latest_measurements_date.alias("ts"),
            on=["metering_point_id"],
            how="left",
        )
        .select(
            "cenc.*",
            F.col("ts.latest_observation_date").alias("last_run"),
        )
    )

    df = cenc_w_last_run.select(
        "*",
        F.explode(
            F.sequence(
                F.date_add(F.col("last_run"), 1), F.col("transaction_creation_datetime"), F.expr("INTERVAL 1 DAY")
            )
        ).alias("date"),
    )

    result_df = df.select(
        F.col("metering_point_id"),
        F.col("metering_point_type"),
        F.col("date"),
        F.col("quantity"),
    )

    print("result_df")
    result_df.show()

    result_df = convert_to_utc(result_df, time_zone)

    return result_df
