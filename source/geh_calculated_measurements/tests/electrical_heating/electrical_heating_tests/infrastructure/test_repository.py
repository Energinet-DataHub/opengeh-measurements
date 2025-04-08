from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.infrastructure import (
    ElectricityMarketRepository,
)

_TEST_FILES_FOLDER_PATH = str(Path(__file__).parent / "test_data")


def _create_repository_with_extra_col(spark: SparkSession) -> ElectricityMarketRepository:
    return ElectricityMarketRepository(
        spark,
        _TEST_FILES_FOLDER_PATH,
        consumption_metering_point_periods_file_name="consumption_metering_point_periods_v1_extra_col.csv",
        child_metering_points_file_name="child_metering_points_v1_extra_col.csv",
    )


def _create_repository_with_missing_col(spark: SparkSession) -> ElectricityMarketRepository:
    return ElectricityMarketRepository(
        spark,
        _TEST_FILES_FOLDER_PATH,
        consumption_metering_point_periods_file_name="consumption_metering_point_periods_v1_missing_col.csv",
        child_metering_points_file_name="child_metering_points_v1_missing_col.csv",
    )


def _create_repository(spark: SparkSession) -> ElectricityMarketRepository:
    return ElectricityMarketRepository(spark, _TEST_FILES_FOLDER_PATH)


def test__when_child_missing_expected_column_raises_exception(spark: SparkSession, monkeypatch) -> None:
    with pytest.raises(ValueError, match=r"Column metering_point_sub_type not found in CSV"):
        _create_repository_with_missing_col(spark).read_child_metering_points()


def test__when_consumption_source_contains_unexpected_columns_returns_data_without_unexpected_column(
    spark: SparkSession,
) -> None:
    # Arrange
    consumption = _create_repository(spark).read_consumption_metering_point_periods()

    # Act
    consumption_with_extra_input_col = _create_repository_with_extra_col(
        spark
    ).read_consumption_metering_point_periods()

    # Assert
    assert consumption_with_extra_input_col.df.columns == consumption.df.columns


def test__when_child_source_contains_unexpected_columns_returns_data_without_unexpected_column(
    spark: SparkSession,
) -> None:
    # Arrange
    child = _create_repository(spark).read_child_metering_points()

    # Act
    child_with_extra_input_col = _create_repository_with_extra_col(spark).read_child_metering_points()

    # Assert
    assert child_with_extra_input_col.df.columns == child.df.columns


# should be use if this repository reads from a source that contains data type e.g. delta table
@pytest.mark.skip(reason="csv file does not contain data type")
def test__when_source_contains_wrong_data_type_raises_exception(
    electricity_market_repository_wrong_data_type: ElectricityMarketRepository,
) -> None:
    pass
