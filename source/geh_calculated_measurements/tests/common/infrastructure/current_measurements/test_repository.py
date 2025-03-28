import datetime
from decimal import Decimal

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure import CurrentMeasurementsRepository
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)


@pytest.fixture(scope="module")
def current_measurements_repository(spark: SparkSession) -> CurrentMeasurementsRepository:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {MeasurementsGoldDatabaseDefinition.DATABASE_NAME}")
    return CurrentMeasurementsRepository(
        spark=spark,
        catalog_name=spark.catalog.currentCatalog(),
    )


@pytest.fixture(scope="module")
def valid_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [("123456789012345678", "consumption", Decimal("1.123"), "measured", datetime.datetime(2023, 1, 1, 0, 0, 0))],
        CurrentMeasurements.schema,
    )


def test__when_missing_expected_column_raises_exception(
    current_measurements_repository: CurrentMeasurementsRepository,
    valid_df: DataFrame,
) -> None:
    # Arrange
    invalid_df = valid_df.drop(F.col("quantity"))

    (
        invalid_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
        )
    )

    # Act & Assert
    with pytest.raises(
        Exception,
        match=r"\[UNRESOLVED_COLUMN\.WITH_SUGGESTION\].*",
    ):
        current_measurements_repository.read_current_measurements()


def test__when_source_contains_unexpected_columns_returns_data_without_unexpected_column(
    current_measurements_repository: CurrentMeasurementsRepository,
    valid_df: DataFrame,
) -> None:
    # Arrange
    valid_df_with_extra_col = valid_df.withColumn("extra_col", F.lit("extra_value"))

    (
        valid_df_with_extra_col.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
        )
    )

    # Act
    col_with_extra = current_measurements_repository.read_current_measurements().df.columns

    # Assert
    assert col_with_extra == valid_df.columns


def test__when_source_contains_wrong_data_type_raises_exception(
    current_measurements_repository: CurrentMeasurementsRepository,
    valid_df: DataFrame,
) -> None:
    # Arrange
    invalid_df = valid_df.withColumn("metering_point_id", F.col("metering_point_id").cast(T.IntegerType()))
    (
        invalid_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
        )
    )

    # Act & Assert
    with pytest.raises(
        AssertionError,
        match=r"Schema mismatch",
    ):
        current_measurements_repository.read_current_measurements()
