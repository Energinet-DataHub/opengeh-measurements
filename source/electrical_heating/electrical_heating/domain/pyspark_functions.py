from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def convert_utc_to_localtime(
    df: DataFrame, timestamp_column: str, time_zone: str
) -> DataFrame:
    return df.withColumn(
        timestamp_column,
        F.from_utc_timestamp(F.col(timestamp_column), time_zone),
    )
