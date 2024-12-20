from pyspark.sql import DataFrame
import pyspark.sql.functions as F


# TODO: switch to using `.select` instead of `.withColumn` for better performance
def convert_utc_to_localtime(
    df: DataFrame, timestamp_column: str, time_zone: str
) -> DataFrame:
    return df.withColumn(
        timestamp_column,
        F.from_utc_timestamp(F.col(timestamp_column), time_zone),
    )


# TODO: switch to using `.select` instead of `.withColumn` for better performance
def convert_localtime_to_utc(
    df: DataFrame, timestamp_column: str, time_zone: str
) -> DataFrame:
    return df.withColumn(
        timestamp_column,
        F.to_utc_timestamp(F.col(timestamp_column), time_zone),
    )
