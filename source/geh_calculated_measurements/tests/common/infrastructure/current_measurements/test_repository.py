import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CurrentMeasurementsRepository
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)


@pytest.fixture(scope="session")
def current_measurements_repository(spark: SparkSession) -> CurrentMeasurementsRepository:
    return CurrentMeasurementsRepository(
        spark=spark,
        catalog_name=spark.catalog.currentCatalog(),
    )


def test__when_missing_expected_column_raises_exception(
    current_measurements_repository: CurrentMeasurementsRepository,
) -> None:
    # Arrange
    df = current_measurements_repository._spark.read.table(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
    )
    df = df.drop(F.col("quantity"))

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
        )
    )

    # Assert
    with pytest.raises(
        Exception,
        match=r"\[UNRESOLVED_COLUMN\.WITH_SUGGESTION\].*",
    ):
        current_measurements_repository.read_current_measurements()


def test__when_source_contains_unexpected_columns_returns_data_without_unexpected_column(
    current_measurements_repository: CurrentMeasurementsRepository,
) -> None:
    # Arrange
    df_original = current_measurements_repository._spark.read.table(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
    )
    col_original = df_original.columns
    df = df_original.withColumn("extra_col", F.lit("extra_value"))

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
        )
    )

    # Act
    col_with_extra = current_measurements_repository.read_current_measurements().df.columns

    # Assert
    assert col_with_extra == col_original


def test__when_source_contains_wrong_data_type_raises_exception(
    current_measurements_repository: CurrentMeasurementsRepository,
) -> None:
    # Arrange
    df = current_measurements_repository._spark.read.table(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
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
            f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
        )
    )

    # Assert
    with pytest.raises(
        AssertionError,
        match=r"Schema mismatch",
    ):
        current_measurements_repository.read_current_measurements()
