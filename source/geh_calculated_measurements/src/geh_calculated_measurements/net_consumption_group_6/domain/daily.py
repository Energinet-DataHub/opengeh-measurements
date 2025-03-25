import pyspark.sql.functions as F
from geh_common.domain.types import MeteringPointType, OrchestrationType
from geh_common.pyspark.transformations import convert_from_utc, convert_to_utc
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import Column
from pyspark.sql import types as T

from geh_calculated_measurements.capacity_settlement.domain import TimeSeriesPoints
from geh_calculated_measurements.common.domain import (
    CalculatedMeasurements,
    ContractColumnNames,
    calculated_measurements_factory,
)
from geh_calculated_measurements.net_consumption_group_6.domain.cenc import Cenc


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


from geh_calculated_measurements.net_consumption_group_6.domain import (
    Cenc,
    TimeSeriesPoints,
)


@use_span()
@testing()
def calculate_daily(
    cenc: Cenc,
    time_series_points: TimeSeriesPoints,
    time_zone: str,
    execution_start_datetime: str,
    orchestration_instance_id: str,
) -> CalculatedMeasurements:
    cenc_added_col = cenc.df.select(  # adding needed columns
        "*",
        F.lit(OrchestrationType.NET_CONSUMPTION.value).alias(ContractColumnNames.orchestration_type),
        F.lit(MeteringPointType.NET_CONSUMPTION.value).alias(ContractColumnNames.metering_point_type),
        F.lit(execution_start_datetime).alias("transaction_creation_datetime").cast(T.TimestampType()),
        F.current_timestamp().alias("now"),
    )

    print("Schema of df:")
    cenc_added_col.printSchema()

    print("added columns:")
    cenc_added_col.show()

    time_series_points_df = time_series_points.df

    cenc_added_col = convert_from_utc(cenc_added_col, time_zone)
    time_series_points_df = convert_from_utc(time_series_points_df, time_zone)

    print("Schema of time_series_points_df:")
    time_series_points_df.printSchema()

    cenc_selected_col = cenc_added_col.select(  # selecting needed columns
        F.col("orchestration_type"),
        F.col("transaction_creation_datetime"),
        F.col("orchestration_instance_id"),
        F.col("metering_point_id"),
        F.col("metering_point_type"),
        (F.col("quantity") / days_in_year(F.col("settlement_year"), F.col("settlement_month")))
        .cast(T.DecimalType(18, 3))
        .alias("quantity"),
        F.col("now"),
    )

    latest_measurements_date = (
        time_series_points_df.where(F.col("metering_point_type") == MeteringPointType.NET_CONSUMPTION.value)
        .groupBy("metering_point_id")
        .agg(F.max("observation_time").alias("latest_observation_date"))
    )

    print("df after selecting needed columns:")
    latest_measurements_date.show()

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

    print("df after joining with internal_daily:")
    cenc_w_last_run.show()

    df = cenc_w_last_run.select(
        "*",
        F.explode(F.sequence(F.date_add(F.col("last_run"), 1), F.col("now"), F.expr("INTERVAL 1 DAY"))).alias("date"),
    )

    result_df = df.select(
        F.col("orchestration_type"),
        F.col("orchestration_instance_id"),
        F.lit("to be added").alias("transaction_id"),
        F.col("transaction_creation_datetime"),
        F.col("metering_point_id"),
        F.col("metering_point_type"),
        F.col("date"),
        F.col("quantity"),
    )

    print("result df:")
    result_df.show()

    result_df = convert_to_utc(result_df, time_zone)

    calculated_measurements = calculated_measurements_factory.create(
        measurements=result_df,
        orchestration_instance_id=orchestration_instance_id,
        orchestration_type=OrchestrationType.NET_CONSUMPTION,
        metering_point_type=MeteringPointType.NET_CONSUMPTION,
        time_zone=time_zone,
    )

    return calculated_measurements
