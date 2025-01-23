from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode

from gold.domain.constants.column_names.gold_measurements_column_names import (
    GoldMeasurementsColumnNames,
)
from gold.domain.constants.column_names.silver_measurements_column_names import (
    SilverMeasurementsColumnNames,
)


def explode_silver_points(df: DataFrame) -> DataFrame:
    return df.select(
        "*",
        explode(SilverMeasurementsColumnNames.points).alias("point")
    ).select(
        df.columns + [
            col(f"point.{SilverMeasurementsColumnNames.Points.quantity}").alias(GoldMeasurementsColumnNames.quantity),
            col(f"point.{SilverMeasurementsColumnNames.Points.quality}").alias(GoldMeasurementsColumnNames.quality),
        ]
    ).drop(SilverMeasurementsColumnNames.points)
