import pyspark.sql.functions as F
from geh_common.pyspark.transformations import convert_from_utc, convert_to_utc
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import Column, DataFrame
from pyspark.sql import types as T

from geh_calculated_measurements.common.domain import CalculatedMeasurements
from geh_calculated_measurements.net_consumption_group_6.domain.cenc import Cenc


def days_in_year(year: Column) -> Column:
    # Create date for January 1st of the given year (for Column input)
    start_date = F.to_date(F.concat(year.cast(T.StringType()), F.lit("-01-01")))
    # Create date for January 1st of the next year
    end_date = F.to_date(F.concat((year + 1).cast(T.StringType()), F.lit("-01-01")))

    # Calculate the difference in days
    return F.datediff(end_date, start_date)


@use_span()
@testing()
def calculate_daily(
    cenc: Cenc,
    time_zone: str,
    internal_daily: DataFrame,
) -> CalculatedMeasurements:
    # adding needed columns
    df = cenc.df.select(
        "*",
        F.lit("net_consumption").alias("orchestration_type"),
        F.lit("net_consumption").alias("metering_point_type"),
        F.lit("ignore").alias("transaction_id"),
        F.lit("2024-01-31T23:00:00Z").alias("transaction_creation_datetime").cast(T.TimestampType()),
        F.current_timestamp().alias("now"),
    )

    print("Schema of df:")
    df.printSchema()

    print("added columns:")
    df.show()

    df = convert_from_utc(df, time_zone)
    internal_daily = convert_from_utc(internal_daily, time_zone)

    # selecting needed columns
    df = df.select(
        F.col("orchestration_type"),
        F.col("transaction_id"),
        F.col("transaction_creation_datetime"),
        F.col("orchestration_instance_id"),
        F.col("metering_point_id"),
        F.col("metering_point_type"),
        (F.col("quantity") / days_in_year(F.year(F.col("now"))).cast(T.DecimalType(18, 3)))
        .alias("quantity")
        .cast(T.DecimalType(18, 3)),
        F.col("now"),
    )

    print("df after selecting needed columns:")
    df.show()

    print("internal_daily:")
    internal_daily.show()

    # merging the with internal
    df = df.join(
        internal_daily,
        on=["metering_point_id"],
        how="inner",
    )

    print("df after joining with internal_daily:")
    df.show()

    df = df.select(
        "*",
        F.explode(F.sequence(F.date_add(F.col("last_run"), 1), F.col("now"), F.expr("INTERVAL 1 DAY"))).alias("date"),
    )

    print("result df:")
    df.show()

    result_df = convert_to_utc(df, time_zone)

    return CalculatedMeasurements(result_df)
