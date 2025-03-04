import random

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from pyspark.errors.exceptions.captured import AnalysisException

from geh_calculated_measurements.electrical_heating.infrastructure import (
    MeasurementsGoldDatabaseDefinition,
    MeasurementsGoldRepository,
)


def test__when_missing_expected_column_raises_exception(
    measurements_gold_repository: MeasurementsGoldRepository,
) -> None:
    # Read the existing table data
    df = measurements_gold_repository._spark.read.table(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}_base"
    )
    # Drop a random column
    random_column = random.choice(df.columns)
    df = df.drop(random_column)

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}"
        )
    )
    with pytest.raises(AnalysisException):
        measurements_gold_repository.read_time_series_points()


def test__when_source_contains_unexpected_columns_returns_data_without_unexpected_column(
    measurements_gold_repository: MeasurementsGoldRepository,
) -> None:
    # Read the existing table data
    df_original = measurements_gold_repository._spark.read.table(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}_base"
    )
    # Add a random column
    df = df_original.withColumn("extra_col", F.lit("extra_value"))

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}"
        )
    )

    col_with_extra = measurements_gold_repository.read_time_series_points().df.columns
    col_original = df_original.columns
    assert col_with_extra == col_original


def test__when_source_contains_wrong_data_type_raises_exception(
    measurements_gold_repository: MeasurementsGoldRepository,
) -> None:
    # Read the existing table data
    df = measurements_gold_repository._spark.read.table(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}_base"
    )
    df = df.select(
        F.col("metering_point_id"),
        F.col("metering_point_type"),
        F.col("observation_time"),
        F.col("quantity").cast(T.StringType()),  # <- Change the data type from DecimalType to StringType
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}"
        )
    )

    with pytest.raises(
        AssertionError,
        match=r"Schema mismatch\. Expected column name 'quantity' to have type DecimalType\(18,3\), but got type StringType\(\)",
    ):
        measurements_gold_repository.read_time_series_points()
