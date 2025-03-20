import pyspark.sql.functions as F
from geh_common.pyspark.transformations import convert_to_utc
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import Column, DataFrame
from pyspark.sql import types as T

from geh_calculated_measurements.common.domain import CalculatedMeasurements
from geh_calculated_measurements.net_consumption_group_6.domain.cenc import Cenc


def days_in_year(year: int) -> Column:
    # Create date for January 1st of the given year
    start_date = F.lit(f"{year}-01-01").cast(T.DateType())
    # Create date for January 1st of the next year
    end_date = F.lit(f"{year + 1}-01-01").cast(T.DateType())
    # Calculate the difference in days
    return F.datediff(end_date, start_date)


@use_span()
@testing()
def calculate_daily(
    cenc: Cenc,
    calculation_year: int,
    time_zone: str,
    internal_daily: DataFrame,
) -> CalculatedMeasurements:
    # adding needed columns
    df = cenc.df.select(
        "*",
        days_in_year(calculation_year).alias("days_in_year"),
        F.lit("net_consumption").alias("orchestration_type"),
        F.lit("net_consumption").alias("metering_point_type"),
        F.lit("ignore").alias("transaction_id"),
        F.lit("2024-01-31T23:00:00Z").alias("transaction_creation_datetime").cast(T.TimestampType()),
        F.col("quantity").cast(T.DecimalType(18, 3)),
    )

    df.show()

    # selecting needed columns
    df = df.select(
        F.col("orchestration_type"),
        F.col("transaction_id"),
        F.col("transaction_creation_datetime"),
        F.col("orchestration_instance_id"),
        F.col("metering_point_id"),
        F.col("metering_point_type"),
        (F.col("quantity") / F.col("days_in_year")).alias("quantity").cast(T.DecimalType(18, 3)),
        F.current_timestamp().alias("now"),
    )

    # merging the with internal
    df = df.join(
        internal_daily,
        on=["metering_point_id"],
        how="inner",
    ).select(
        "*",
        F.explode(F.sequence(F.col("last_run"), F.col("now"), F.expr("INTERVAL 1 DAY"))).alias("date"),
    )

    ## todo: patch F.current_timestamp

    result_df = convert_to_utc(df, time_zone)

    return CalculatedMeasurements(result_df)
