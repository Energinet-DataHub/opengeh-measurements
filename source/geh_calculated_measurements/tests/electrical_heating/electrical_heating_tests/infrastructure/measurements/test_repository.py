import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest

from geh_calculated_measurements.electrical_heating.infrastructure import (
    MeasurementsGoldDatabaseDefinition,
    MeasurementsGoldRepository,
)


def test__when_missing_expected_column_raises_exception(
    measurements_gold_repository: MeasurementsGoldRepository,
) -> None:
    # Arrange
    df = measurements_gold_repository._spark.read.table(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}"
    )
    df = df.drop(F.col("quantity"))

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}"
        )
    )

    # Assert
    with pytest.raises(
        Exception,
        match=r"\[UNRESOLVED_COLUMN\.WITH_SUGGESTION\] A column or function parameter with name `quantity` cannot be resolved\. Did you mean one of the following\?.*",
    ):
        measurements_gold_repository.read_time_series_points()


def test__when_source_contains_unexpected_columns_returns_data_without_unexpected_column(
    measurements_gold_repository: MeasurementsGoldRepository,
) -> None:
    # Arrange
    df_original = measurements_gold_repository._spark.read.table(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}"
    )
    col_original = df_original.columns
    df = df_original.withColumn("extra_col", F.lit("extra_value"))

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}"
        )
    )

    # Act
    col_with_extra = measurements_gold_repository.read_time_series_points().df.columns

    # Assert
    assert col_with_extra == col_original


def test__when_source_contains_wrong_data_type_raises_exception(
    measurements_gold_repository: MeasurementsGoldRepository,
) -> None:
    # Arrange
    df = measurements_gold_repository._spark.read.table(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}"
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

    # Assert
    with pytest.raises(
        AssertionError,
        match=r"Schema mismatch\. Expected column name 'quantity' to have type DecimalType\(18,3\), but got type StringType\(\)",
    ):
        measurements_gold_repository.read_time_series_points()
