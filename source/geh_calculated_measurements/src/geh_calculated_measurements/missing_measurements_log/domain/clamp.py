from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def clamp_period(
    df: DataFrame,
    clamp_start_datetime: datetime,
    clamp_end_datetime: datetime,
    period_start_column_name: str,
    period_end_column_name: str,
) -> DataFrame:
    df = df.withColumn(
        period_start_column_name,
        F.when(F.col(period_start_column_name) < clamp_start_datetime, clamp_start_datetime).otherwise(
            F.col(period_start_column_name)
        ),
    ).withColumn(
        period_end_column_name,
        F.when(
            F.col(period_end_column_name).isNull() | (F.col(period_end_column_name) > clamp_end_datetime),
            clamp_end_datetime,
        ).otherwise(F.col(period_end_column_name)),
    )

    return df
