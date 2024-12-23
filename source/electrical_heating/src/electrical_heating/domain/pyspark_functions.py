from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def convert_utc_to_localtime(
    df: DataFrame, timestamp_column: str, time_zone: str
) -> DataFrame:
    return (
        df.select(
            "*",
            F.from_utc_timestamp(F.col(timestamp_column), time_zone).alias(
                f"{timestamp_column}_tmp"
            ),
        )
        .drop(timestamp_column)
        .withColumnRenamed(f"{timestamp_column}_tmp", timestamp_column)
    )


def convert_localtime_to_utc(
    df: DataFrame, timestamp_column: str, time_zone: str
) -> DataFrame:
    return (
        df.select(
            "*",
            F.to_utc_timestamp(F.col(timestamp_column), time_zone).alias(
                f"{timestamp_column}_tmp"
            ),
        )
        .drop(timestamp_column)
        .withColumnRenamed(f"{timestamp_column}_tmp", timestamp_column)
    )
