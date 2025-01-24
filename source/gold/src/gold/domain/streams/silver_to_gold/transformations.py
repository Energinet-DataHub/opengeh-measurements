import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from gold.domain.constants.column_names.gold_measurements_column_names import (
    GoldMeasurementsColumnNames,
)
from gold.domain.constants.column_names.silver_measurements_column_names import (
    SilverMeasurementsColumnNames,
)


def transform_silver_to_gold(df: DataFrame) -> DataFrame:
    exploded_df = explode_silver_points(df)

    return exploded_df.select(
        F.col(SilverMeasurementsColumnNames.metering_point_id).alias(GoldMeasurementsColumnNames.metering_point_id),
        F.col(SilverMeasurementsColumnNames.start_datetime).alias(GoldMeasurementsColumnNames.observation_time),
        F.col(f"col.{SilverMeasurementsColumnNames.Points.quantity}").alias(GoldMeasurementsColumnNames.quantity),
        F.col(f"col.{SilverMeasurementsColumnNames.Points.quality}").alias(GoldMeasurementsColumnNames.quality),
        F.col(SilverMeasurementsColumnNames.metering_point_type).alias(GoldMeasurementsColumnNames.metering_point_type),
        F.col(SilverMeasurementsColumnNames.transaction_id).alias(GoldMeasurementsColumnNames.transaction_id),
        F.col(SilverMeasurementsColumnNames.transaction_creation_datetime).alias(
            GoldMeasurementsColumnNames.transaction_creation_datetime
        ),
        F.current_timestamp().alias(GoldMeasurementsColumnNames.created),
        F.current_timestamp().alias(GoldMeasurementsColumnNames.modified),
    )


def explode_silver_points(df: DataFrame) -> DataFrame:
    return df.select("*", F.explode(F.col(SilverMeasurementsColumnNames.points))).drop(
        SilverMeasurementsColumnNames.points
    )
